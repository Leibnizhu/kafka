/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.consumer

import kafka.server.{AbstractFetcherManager, AbstractFetcherThread, BrokerAndInitialOffset}
import kafka.cluster.{BrokerEndPoint, Cluster}
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time

import scala.collection.immutable
import collection.mutable.HashMap
import scala.collection.mutable
import java.util.concurrent.locks.ReentrantLock

import kafka.utils.CoreUtils.inLock
import kafka.utils.ZkUtils
import kafka.utils.ShutdownableThread
import kafka.client.ClientUtils
import java.util.concurrent.atomic.AtomicInteger

/**
 *  Usage:
 *  Once ConsumerFetcherManager is created, startConnections() and stopAllConnections() can be called repeatedly
 *  until shutdown() is called.
  *  消费者拉取线程管理器
 */
class ConsumerFetcherManager(private val consumerIdString: String,
                             private val config: ConsumerConfig,
                             private val zkUtils : ZkUtils)
        extends AbstractFetcherManager("ConsumerFetcherManager-%d".format(Time.SYSTEM.milliseconds),
                                       config.clientId, config.numConsumerFetchers) {
  private var partitionMap: immutable.Map[TopicPartition, PartitionTopicInfo] = null //被消费者拉取管理器所管理的分区集合
  private var cluster: Cluster = null
  private val noLeaderPartitionSet = new mutable.HashSet[TopicPartition] //初始化时假设所有分区都没有主副本
  private val lock = new ReentrantLock
  private val cond = lock.newCondition()
  private var leaderFinderThread: ShutdownableThread = null
  private val correlationId = new AtomicInteger(0)

  //查找有主副本的分区的线程
  private class LeaderFinderThread(name: String) extends ShutdownableThread(name) {
    // thread responsible for adding the fetcher to the right broker when leader is available
    override def doWork() {
      val leaderForPartitionsMap = new HashMap[TopicPartition, BrokerEndPoint]
      lock.lock()
      try {
        while (noLeaderPartitionSet.isEmpty) { //没数据就等待
          trace("No partition for leader election.")
          cond.await()
        }

        trace("Partitions without leader %s".format(noLeaderPartitionSet))
        val brokers = ClientUtils.getPlaintextBrokerEndPoints(zkUtils)
        //向Kafka任一消息代理节点发起主题元数据请求
        //一个主题有多个分区，主题元数据也包括了所有分区的元数据，包括每个分区的主副本信息
        val topicsMetadata = ClientUtils.fetchTopicMetadata(noLeaderPartitionSet.map(m => m.topic).toSet,
                                                            brokers,
                                                            config.clientId,
                                                            config.socketTimeoutMs,
                                                            correlationId.getAndIncrement).topicsMetadata
        if(logger.isDebugEnabled) topicsMetadata.foreach(topicMetadata => debug(topicMetadata.toString()))
        topicsMetadata.foreach { tmd => //遍历主题元数据
          val topic = tmd.topic
          tmd.partitionsMetadata.foreach { pmd => //遍历分区元数据
            val topicAndPartition = new TopicPartition(topic, pmd.partitionId)
            //当前分区存在主副本(leader),且在候选集合中,则从集合中移除,并加入到主副本的Map中
            if(pmd.leader.isDefined && noLeaderPartitionSet.contains(topicAndPartition)) {
              val leaderBroker = pmd.leader.get
              leaderForPartitionsMap.put(topicAndPartition, leaderBroker)
              noLeaderPartitionSet -= topicAndPartition
            }
          }
        }
      } catch {
        case t: Throwable => {
            if (!isRunning.get())
              throw t /* If this thread is stopped, propagate this exception to kill the thread. */
            else
              warn("Failed to find leader for %s".format(noLeaderPartitionSet), t)
          }
      } finally {
        lock.unlock()
      }

      try {
        //将分区加入到拉取线程,BrokerAndInitialOffset会指定拉取线程从什么偏移量开始拉取信息
        addFetcherForPartitions(leaderForPartitionsMap.map { case (topicPartition, broker) =>
          topicPartition -> BrokerAndInitialOffset(broker, partitionMap(topicPartition).getFetchOffset())} //话题分区->(broker节点,偏移量)
        )
      } catch {
        case t: Throwable => {
          if (!isRunning.get())
            throw t /* If this thread is stopped, propagate this exception to kill the thread. */
          else {
            warn("Failed to add leader for partitions %s; will retry".format(leaderForPartitionsMap.keySet.mkString(",")), t)
            lock.lock()
            noLeaderPartitionSet ++= leaderForPartitionsMap.keySet
            lock.unlock()
          }
        }
      }

      shutdownIdleFetcherThreads() //删除无分区的拉取线程
      Thread.sleep(config.refreshLeaderBackoffMs)
    }
  }

  //目标节点从sourceBroker获取,偏移量从partitionMap的value获取(即再平衡时从zk获取)
  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread = {
    new ConsumerFetcherThread(
      "ConsumerFetcherThread-%s-%d-%d".format(consumerIdString, fetcherId, sourceBroker.id),
      config, sourceBroker, partitionMap, this)
  }

  //ZookeeperConsumerConnector中,在再平衡后,调用
  def startConnections(topicInfos: Iterable[PartitionTopicInfo], cluster: Cluster) {
    leaderFinderThread = new LeaderFinderThread(consumerIdString + "-leader-finder-thread") //负责找出已经存在主副本的分区,选中的分区会被加入对应的拉取线程
    leaderFinderThread.start()

    inLock(lock) {
      partitionMap = topicInfos.map(tpi => (new TopicPartition(tpi.topic, tpi.partitionId), tpi)).toMap
      this.cluster = cluster
      noLeaderPartitionSet ++= topicInfos.map(tpi => new TopicPartition(tpi.topic, tpi.partitionId))
      cond.signalAll()
    }
  }

  def stopConnections() {
    /*
     * Stop the leader finder thread first before stopping fetchers. Otherwise, if there are more partitions without
     * leader, then the leader finder thread will process these partitions (before shutting down) and add fetchers for
     * these partitions.
     */
    info("Stopping leader finder thread")
    if (leaderFinderThread != null) {
      leaderFinderThread.shutdown()
      leaderFinderThread = null
    }

    info("Stopping all fetchers")
    closeAllFetchers()

    // no need to hold the lock for the following since leaderFindThread and all fetchers have been stopped
    partitionMap = null
    noLeaderPartitionSet.clear()

    info("All connections stopped")
  }

  def addPartitionsWithError(partitionList: Iterable[TopicPartition]) {
    debug("adding partitions with error %s".format(partitionList))
    inLock(lock) {
      if (partitionMap != null) {
        noLeaderPartitionSet ++= partitionList
        cond.signalAll()
      }
    }
  }
}
