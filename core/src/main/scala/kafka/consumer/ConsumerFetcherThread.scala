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

import kafka.api.{FetchRequestBuilder, FetchResponsePartitionData, OffsetRequest, Request}
import kafka.cluster.BrokerEndPoint
import kafka.message.ByteBufferMessageSet
import kafka.server.{AbstractFetcherThread, PartitionFetchState}
import kafka.common.{ErrorMapping, TopicAndPartition}

import scala.collection.Map
import ConsumerFetcherThread._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.MemoryRecords

class ConsumerFetcherThread(name: String,
                            val config: ConsumerConfig,
                            sourceBroker: BrokerEndPoint,
                            partitionMap: Map[TopicPartition, PartitionTopicInfo], //拉取线程管理器持有的partitionMap引用
                            val consumerFetcherManager: ConsumerFetcherManager)
        extends AbstractFetcherThread(name = name,
                                      clientId = config.clientId,
                                      sourceBroker = sourceBroker,
                                      fetchBackOffMs = config.refreshLeaderBackoffMs,
                                      isInterruptible = true) {

  type REQ = FetchRequest
  type PD = PartitionData

  private val clientId = config.clientId
  private val fetchSize = config.fetchMessageMaxBytes

  private val simpleConsumer = new SimpleConsumer(sourceBroker.host, sourceBroker.port, config.socketTimeoutMs,
    config.socketReceiveBufferBytes, config.clientId) //代表和服务端的网络连接

  private val fetchRequestBuilder = new FetchRequestBuilder().
    clientId(clientId).
    replicaId(Request.OrdinaryConsumerId). //-1,消费者没有replicaId
    maxWait(config.fetchWaitMaxMs). //拉取请求的最长等待时间
    minBytes(config.fetchMinBytes). //拉取请求的最少字节大小
    requestVersion(kafka.api.FetchRequest.CurrentVersion) //拉取请求的api版本

  override def initiateShutdown(): Boolean = {
    val justShutdown = super.initiateShutdown()
    if (justShutdown && isInterruptible)
      simpleConsumer.disconnectToHandleJavaIOBug()
    justShutdown
  }

  override def shutdown(): Unit = {
    super.shutdown()
    simpleConsumer.close()
  }

  // process fetched data
  //传入分区数据的底层消息集
  def processPartitionData(topicPartition: TopicPartition, fetchOffset: Long, partitionData: PartitionData) {
    val pti = partitionMap(topicPartition)
    if (pti.getFetchOffset != fetchOffset)
      throw new RuntimeException("Offset doesn't match for partition [%s,%d] pti offset: %d fetch offset: %d"
                                .format(topicPartition.topic, topicPartition.partition, pti.getFetchOffset, fetchOffset))
    pti.enqueue(partitionData.underlying.messages.asInstanceOf[ByteBufferMessageSet]) //将消息集包装成数据块放入法恩去信息对象的队列中
  }

  // handle a partition whose offset is out of range and return a new fetch offset
  //拉取请求的偏移量超了
  def handleOffsetOutOfRange(topicPartition: TopicPartition): Long = {
    val startTimestamp = config.autoOffsetReset match {
      case OffsetRequest.SmallestTimeString => OffsetRequest.EarliestTime
      case OffsetRequest.LargestTimeString => OffsetRequest.LatestTime
      case _ => OffsetRequest.LatestTime
    }
    val topicAndPartition = TopicAndPartition(topicPartition.topic, topicPartition.partition)
    val newOffset = simpleConsumer.earliestOrLatestOffset(topicAndPartition, startTimestamp, Request.OrdinaryConsumerId) //重新计算偏移量
    val pti = partitionMap(topicPartition)
    //重置分区信息对象的两个偏移量
    pti.resetFetchOffset(newOffset)
    pti.resetConsumeOffset(newOffset)
    newOffset
  }

  // any logic for partitions whose leader has changed
  def handlePartitionsWithErrors(partitions: Iterable[TopicPartition]) {
    removePartitions(partitions.toSet)
    consumerFetcherManager.addPartitionsWithError(partitions)
  }

  protected def buildFetchRequest(partitionMap: collection.Seq[(TopicPartition, PartitionFetchState)]): FetchRequest = {
    partitionMap.foreach { case ((topicPartition, partitionFetchState)) =>
      if (partitionFetchState.isActive)
        fetchRequestBuilder.addFetch(topicPartition.topic, topicPartition.partition, partitionFetchState.offset,
          fetchSize)
    }
    //构造器模式
    new FetchRequest(fetchRequestBuilder.build())
  }

  protected def fetch(fetchRequest: FetchRequest): Seq[(TopicPartition, PartitionData)] =
    simpleConsumer.fetch(fetchRequest.underlying).data.map { case (TopicAndPartition(t, p), value) =>
      new TopicPartition(t, p) -> new PartitionData(value) //使用simpleConsumer向服务端发出请求并返回所有分区及其数据,再map转换类型
    }
}

object ConsumerFetcherThread {

  class FetchRequest(val underlying: kafka.api.FetchRequest) extends AbstractFetcherThread.FetchRequest {
    private lazy val tpToOffset: Map[TopicPartition, Long] = underlying.requestInfo.map { case (tp, fetchInfo) =>
      new TopicPartition(tp.topic, tp.partition) -> fetchInfo.offset
    }.toMap
    def isEmpty: Boolean = underlying.requestInfo.isEmpty
    def offset(topicPartition: TopicPartition): Long = tpToOffset(topicPartition)
  }

  class PartitionData(val underlying: FetchResponsePartitionData) extends AbstractFetcherThread.PartitionData {
    def errorCode: Short = underlying.error
    def toRecords: MemoryRecords = underlying.messages.asInstanceOf[ByteBufferMessageSet].asRecords
    def highWatermark: Long = underlying.hw
    def exception: Option[Throwable] =
      if (errorCode == ErrorMapping.NoError) None else Some(ErrorMapping.exceptionFor(errorCode))

  }
}
