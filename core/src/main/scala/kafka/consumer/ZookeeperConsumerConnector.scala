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

import java.net.InetAddress
import java.util.UUID
import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks.ReentrantLock

import com.yammer.metrics.core.Gauge
import kafka.api._
import kafka.client.ClientUtils
import kafka.cluster._
import kafka.common._
import kafka.javaapi.consumer.ConsumerRebalanceListener
import kafka.metrics._
import kafka.network.BlockingChannel
import kafka.serializer._
import kafka.utils.CoreUtils.inLock
import kafka.utils.ZkUtils._
import kafka.utils._
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.{IZkChildListener, IZkDataListener, IZkStateListener}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.Watcher.Event.KeeperState

import scala.collection._
import scala.collection.JavaConverters._

/**
 * This class handles the consumers interaction with zookeeper
 *
 * Directories:
 * 1. Consumer id registry:
 * /consumers/[group_id]/ids/[consumer_id] -> topic1,...topicN
 * A consumer has a unique consumer id within a consumer group. A consumer registers its id as an ephemeral znode
 * and puts all topics that it subscribes to as the value of the znode. The znode is deleted when the client is gone.
 * A consumer subscribes to event changes of the consumer id registry within its group.
 *
 * The consumer id is picked up from configuration, instead of the sequential id assigned by ZK. Generated sequential
 * ids are hard to recover during temporary connection loss to ZK, since it's difficult for the client to figure out
 * whether the creation of a sequential znode has succeeded or not. More details can be found at
 * (http://wiki.apache.org/hadoop/ZooKeeper/ErrorHandling)
 *
 * 2. Broker node registry:
 * /brokers/[0...N] --> { "host" : "host:port",
 *                        "topics" : {"topic1": ["partition1" ... "partitionN"], ...,
 *                                    "topicN": ["partition1" ... "partitionN"] } }
 * This is a list of all present broker brokers. A unique logical node id is configured on each broker node. A broker
 * node registers itself on start-up and creates a znode with the logical node id under /brokers. The value of the znode
 * is a JSON String that contains (1) the host name and the port the broker is listening to, (2) a list of topics that
 * the broker serves, (3) a list of logical partitions assigned to each topic on the broker.
 * A consumer subscribes to event changes of the broker node registry.
 *
 * 3. Partition owner registry:
 * /consumers/[group_id]/owner/[topic]/[broker_id-partition_id] --> consumer_node_id
 * This stores the mapping before broker partitions and consumers. Each partition is owned by a unique consumer
 * within a consumer group. The mapping is reestablished after each rebalancing.
 *
 * 4. Consumer offset tracking:
 * /consumers/[group_id]/offsets/[topic]/[broker_id-partition_id] --> offset_counter_value
 * Each consumer tracks the offset of the latest message consumed for each partition.
 * trait ConsumerConnector的默认实现类
 */
private[kafka] object ZookeeperConsumerConnector {
  val shutdownCommand: FetchedDataChunk = new FetchedDataChunk(null, null, -1L)
}

private[kafka] class ZookeeperConsumerConnector(val config: ConsumerConfig,
                                                val enableFetcher: Boolean) // for testing only
        extends ConsumerConnector with Logging with KafkaMetricsGroup {

  private val isShuttingDown = new AtomicBoolean(false)
  private val rebalanceLock = new Object
  private var fetcher: Option[ConsumerFetcherManager] = None //消费者拉去现成的管理类
  private var zkUtils: ZkUtils = null
  private var topicRegistry = new Pool[String, Pool[Int, PartitionTopicInfo]] //消费者分配的分区,主题→(分区→分区信息)”
  private val checkpointedZkOffsets = new Pool[TopicAndPartition, Long]
  private val topicThreadIdAndQueues = new Pool[(String, ConsumerThreadId), BlockingQueue[FetchedDataChunk]]//消费者订阅的主题和线程ID,一个线程对应一个队列
  private val scheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "kafka-consumer-scheduler-")
  private val messageStreamCreated = new AtomicBoolean(false)

  //当新的会话建立或会话超时时需要重新注册消费者,并通过syncedRebalance触发再平衡
  private var sessionExpirationListener: ZKSessionExpireListener = null
  //当主题分区数量变化时,调用rebalanceEventTriggered触发再平衡
  private var topicPartitionChangeListener: ZKTopicPartitionChangeListener = null
  //当消费组成员发生变化时,通过rebalanceEventTriggered触发再平衡
  private var loadBalancerListener: ZKRebalancerListener = null

  private var offsetsChannel: BlockingChannel = null //和管理消费组的协调者通信,一个阻塞通道,一个连接
  private val offsetsChannelLock = new Object

  private var wildcardTopicWatcher: ZookeeperTopicEventWatcher = null
  //一旦触发了再平衡，需要从ZK中读取所有的分区和已注册的消费者
  private var consumerRebalanceListener: ConsumerRebalanceListener = null

  // useful for tracking migration of consumers to store offsets in kafka
  private val kafkaCommitMeter = newMeter("KafkaCommitsPerSec", "commits", TimeUnit.SECONDS, Map("clientId" -> config.clientId))
  private val zkCommitMeter = newMeter("ZooKeeperCommitsPerSec", "commits", TimeUnit.SECONDS, Map("clientId" -> config.clientId))
  private val rebalanceTimer = new KafkaTimer(newTimer("RebalanceRateAndTime", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, Map("clientId" -> config.clientId)))

  newGauge(
    "yammer-metrics-count",
    new Gauge[Int] {
      def value = {
        com.yammer.metrics.Metrics.defaultRegistry().allMetrics().size()
      }
    }
  )

  val consumerIdString = {
    var consumerUuid : String = null
    config.consumerId match {
      case Some(consumerId) // for testing only
      => consumerUuid = consumerId
      case None // generate unique consumerId automatically
      => val uuid = UUID.randomUUID()
      consumerUuid = "%s-%d-%s".format(
        InetAddress.getLocalHost.getHostName, System.currentTimeMillis,
        uuid.getMostSignificantBits().toHexString.substring(0,8))
    }
    config.groupId + "_" + consumerUuid
  }
  this.logIdent = "[" + consumerIdString + "], "

  //确保连接上ZK， 因为消费者要和ZK通信，包括保存消费进度或者读取分区信息等
  connectZk()
  //创建管理所有消费者拉]我线程的消费者拉取管理器
  createFetcher()
  //确保连接上偏移量管理器，消费者保存消费进度到内部主题时和它通信
  ensureOffsetManagerConnected()

  if (config.autoCommitEnable) {
    //调度定时提交偏移量主IJZK或者Kafka内部主题的线程
    scheduler.startup
    info("starting auto committer every " + config.autoCommitIntervalMs + " ms")
    scheduler.schedule("kafka-consumer-autocommit",
                       autoCommit,
                       delay = config.autoCommitIntervalMs,
                       period = config.autoCommitIntervalMs,
                       unit = TimeUnit.MILLISECONDS)
  }

  KafkaMetricsReporter.startReporters(config.props)
  AppInfo.registerInfo()

  def this(config: ConsumerConfig) = this(config, true)

  def createMessageStreams(topicCountMap: Map[String,Int]): Map[String, List[KafkaStream[Array[Byte],Array[Byte]]]] =
    createMessageStreams(topicCountMap, new DefaultDecoder(), new DefaultDecoder())

  def createMessageStreams[K,V](topicCountMap: Map[String,Int], keyDecoder: Decoder[K], valueDecoder: Decoder[V])
      : Map[String, List[KafkaStream[K,V]]] = {
    if (messageStreamCreated.getAndSet(true))
      throw new MessageStreamsExistException(this.getClass.getSimpleName +
                                   " can create message streams at most once",null)
    //并不真正消费数据，而只是为消费消息做准备工作
    consume(topicCountMap, keyDecoder, valueDecoder)
  }

  def createMessageStreamsByFilter[K,V](topicFilter: TopicFilter,
                                        numStreams: Int,
                                        keyDecoder: Decoder[K] = new DefaultDecoder(),
                                        valueDecoder: Decoder[V] = new DefaultDecoder()) = {
    val wildcardStreamsHandler = new WildcardStreamsHandler[K,V](topicFilter, numStreams, keyDecoder, valueDecoder)
    wildcardStreamsHandler.streams
  }

  def setConsumerRebalanceListener(listener: ConsumerRebalanceListener) {
    if (messageStreamCreated.get())
      throw new MessageStreamsExistException(this.getClass.getSimpleName +
        " can only set consumer rebalance listener before creating streams",null)
    consumerRebalanceListener = listener
  }

  private def createFetcher() {
    if (enableFetcher)
      fetcher = Some(new ConsumerFetcherManager(consumerIdString, config, zkUtils))
  }

  private def connectZk() {
    info("Connecting to zookeeper instance at " + config.zkConnect)
    zkUtils = ZkUtils(config.zkConnect,
                      config.zkSessionTimeoutMs,
                      config.zkConnectionTimeoutMs,
                      JaasUtils.isZkSecurityEnabled())
  }

  // Blocks until the offset manager is located and a channel is established to it.
  private def ensureOffsetManagerConnected() {
    if (config.offsetsStorage == "kafka") {
      if (offsetsChannel == null || !offsetsChannel.isConnected)
        offsetsChannel = ClientUtils.channelToOffsetManager(config.groupId, zkUtils,
          config.offsetsChannelSocketTimeoutMs, config.offsetsChannelBackoffMs) //向任意节点发送GroupCoordinatorRequest请求获取消费组对应的协调节点,OffsetManager节点

      debug("Connected to offset manager %s:%d.".format(offsetsChannel.host, offsetsChannel.port))
    }
  }

  def shutdown() {
    val canShutdown = isShuttingDown.compareAndSet(false, true)
    if (canShutdown) {
      info("ZKConsumerConnector shutting down")
      val startTime = System.nanoTime()
      KafkaMetricsGroup.removeAllConsumerMetrics(config.clientId)
      if (wildcardTopicWatcher != null)
        wildcardTopicWatcher.shutdown()
      rebalanceLock synchronized {
        try {
          if (config.autoCommitEnable)
	        scheduler.shutdown()
          fetcher match {
            case Some(f) => f.stopConnections
            case None =>
          }
          sendShutdownToAllQueues()
          if (config.autoCommitEnable)
            commitOffsets(true)
          if (zkUtils != null) {
            zkUtils.close()
            zkUtils = null
          }

          if (offsetsChannel != null) offsetsChannel.disconnect()
        } catch {
          case e: Throwable =>
            fatal("error during consumer connector shutdown", e)
        }
        info("ZKConsumerConnector shutdown completed in " + (System.nanoTime() - startTime) / 1000000 + " ms")
      }
    }
  }

  def consume[K, V](topicCountMap: scala.collection.Map[String,Int], keyDecoder: Decoder[K], valueDecoder: Decoder[V])
      : Map[String,List[KafkaStream[K,V]]] = {
    debug("entering consume ")
    if (topicCountMap == null) //topicCountMap设置主题机器对应的线程个数
      throw new RuntimeException("topicCountMap is null")
    val topicCount = TopicCount.constructTopicCount(consumerIdString, topicCountMap) //consumerId消费者编号=消费组名+随机值

    val topicThreadIds = topicCount.getConsumerThreadIdsPerTopic

    // make a list of (queue,stream) pairs, one pair for each threadId
    //根据客户端传入的 topicCountMap构造对应的队列和消息流2
    val queuesAndStreams = topicThreadIds.values.map(threadIdSet =>
      threadIdSet.map(_ => {
        val queue =  new LinkedBlockingQueue[FetchedDataChunk](config.queuedMaxMessages)
        val stream = new KafkaStream[K,V](
          queue, config.consumerTimeoutMs, keyDecoder, valueDecoder, config.clientId)
        (queue, stream)
      })
    ).flatten.toList

    val dirs = new ZKGroupDirs(config.groupId)
    registerConsumerInZK(dirs, consumerIdString, topicCount) //在ZK的消费组父节点下注册消费者子节点
    reinitializeConsumer(topicCount, queuesAndStreams)//执行初始化工作，触发再平衡，为消费者分配分区，拉取线程会拉取消息放到队列中
    //返回消息流列表
    loadBalancerListener.kafkaMessageAndMetadataStreams.asInstanceOf[Map[String, List[KafkaStream[K,V]]]]
  }

  // this API is used by unit tests only
  def getTopicRegistry: Pool[String, Pool[Int, PartitionTopicInfo]] = topicRegistry

  private def registerConsumerInZK(dirs: ZKGroupDirs, consumerIdString: String, topicCount: TopicCount) {
    info("begin registering consumer " + consumerIdString + " in ZK")
    val timestamp = Time.SYSTEM.milliseconds.toString
    val consumerRegistrationInfo = Json.encode(Map("version" -> 1, "subscription" -> topicCount.getTopicCountMap, "pattern" -> topicCount.pattern,
                                                  "timestamp" -> timestamp))
    val zkWatchedEphemeral = new ZKCheckedEphemeral(dirs.
                                                    consumerRegistryDir + "/" + consumerIdString,
                                                    consumerRegistrationInfo,
                                                    zkUtils.zkConnection.getZookeeper,
                                                    false)
    zkWatchedEphemeral.create()

    info("end registering consumer " + consumerIdString + " in ZK")
  }

  private def sendShutdownToAllQueues() = {
    for (queue <- topicThreadIdAndQueues.values.toSet[BlockingQueue[FetchedDataChunk]]) {
      debug("Clearing up queue")
      queue.clear()
      queue.put(ZookeeperConsumerConnector.shutdownCommand)
      debug("Cleared queue and sent shutdown command")
    }
  }

  def autoCommit() {
    trace("auto committing")
    try {
      commitOffsets(isAutoCommit = false)
    }
    catch {
      case t: Throwable =>
      // log it and let it go
        error("exception during autoCommit: ", t)
    }
  }

  def commitOffsetToZooKeeper(topicPartition: TopicAndPartition, offset: Long) {
    if (checkpointedZkOffsets.get(topicPartition) != offset) {
      val topicDirs = new ZKGroupTopicDirs(config.groupId, topicPartition.topic)
      zkUtils.updatePersistentPath(topicDirs.consumerOffsetDir + "/" + topicPartition.partition, offset.toString)
      checkpointedZkOffsets.put(topicPartition, offset)
      zkCommitMeter.mark()
    }
  }

  /**
   * KAFKA-1743: This method added for backward compatibility.
   */
  def commitOffsets { commitOffsets(true) }

  //在消费者连接器中需要定时的提交偏移量
  def commitOffsets(isAutoCommit: Boolean) {

    val offsetsToCommit =
      immutable.Map(topicRegistry.values.flatMap { partitionTopicInfos => //消费者订阅多个主题
        partitionTopicInfos.values.map { info => //每个主题给当前消费者可能有多个分区,按分区信息拆分
          TopicAndPartition(info.topic, info.partitionId) -> OffsetAndMetadata(info.getConsumeOffset())
        }
      }.toSeq: _*) //flat成序列再转成Map

    commitOffsets(offsetsToCommit, isAutoCommit)

  }

  def commitOffsets(offsetsToCommit: immutable.Map[TopicAndPartition, OffsetAndMetadata], isAutoCommit: Boolean) {
    trace("OffsetMap: %s".format(offsetsToCommit))
    var retriesRemaining = 1 + (if (isAutoCommit) 0 else config.offsetsCommitMaxRetries) // no retries for commits from auto-commit
    var done = false
    while (!done) {
      val committed = offsetsChannelLock synchronized {
        // committed when we receive either no error codes or only MetadataTooLarge errors
        if (offsetsToCommit.size > 0) {
          if (config.offsetsStorage == "zookeeper") { //选择使用zk存储偏移量
            offsetsToCommit.foreach { case (topicAndPartition, offsetAndMetadata) =>
              commitOffsetToZooKeeper(topicAndPartition, offsetAndMetadata.offset) //真正提交到ZK,每个分区都写一次,不同分区的ZK Node不一样
            }
            true
          } else { //使用Kafka内部主题存储偏移量
            val offsetCommitRequest = OffsetCommitRequest(config.groupId, offsetsToCommit, clientId = config.clientId)
            ensureOffsetManagerConnected() //确保客户端连接上偏移量管理器
            try {
              kafkaCommitMeter.mark(offsetsToCommit.size)
              offsetsChannel.send(offsetCommitRequest) //发送提交偏移量的请求
              val offsetCommitResponse = OffsetCommitResponse.readFrom(offsetsChannel.receive().payload())
              trace("Offset commit response: %s.".format(offsetCommitResponse))

              val (commitFailed, retryableIfFailed, shouldRefreshCoordinator, errorCount) = { //处理提交偏移量的响应
                offsetCommitResponse.commitStatus.foldLeft(false, false, false, 0) { case (folded, (topicPartition, errorCode)) =>

                  if (errorCode == Errors.NONE.code && config.dualCommitEnabled) {
                    val offset = offsetsToCommit(topicPartition).offset
                    commitOffsetToZooKeeper(topicPartition, offset)
                  }

                  (folded._1 || // update commitFailed
                    errorCode != Errors.NONE.code,

                    folded._2 || // update retryableIfFailed - (only metadata too large is not retryable)
                      (errorCode != Errors.NONE.code && errorCode != Errors.OFFSET_METADATA_TOO_LARGE.code),

                    folded._3 || // update shouldRefreshCoordinator
                      errorCode == Errors.NOT_COORDINATOR_FOR_GROUP.code ||
                      errorCode == Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code,

                    // update error count
                    folded._4 + (if (errorCode != Errors.NONE.code) 1 else 0))
                }
              }
              debug(errorCount + " errors in offset commit response.")


              if (shouldRefreshCoordinator) {
                debug("Could not commit offsets (because offset coordinator has moved or is unavailable).")
                offsetsChannel.disconnect()
              }

              if (commitFailed && retryableIfFailed)
                false
              else
                true
            }
            catch {
              case t: Throwable =>
                error("Error while committing offsets.", t)
                offsetsChannel.disconnect()
                false
            }
          }
        } else {
          debug("No updates to offsets since last commit.")
          true
        }
      }

      done = {
        retriesRemaining -= 1
        retriesRemaining == 0 || committed
      }

      if (!done) {
        debug("Retrying offset commit in %d ms".format(config.offsetsChannelBackoffMs))
        Thread.sleep(config.offsetsChannelBackoffMs)
      }
    }
  }

  //从zk读取指定分区的偏移量
  private def fetchOffsetFromZooKeeper(topicPartition: TopicAndPartition) = {
    val dirs = new ZKGroupTopicDirs(config.groupId, topicPartition.topic)
    val offsetString = zkUtils.readDataMaybeNull(dirs.consumerOffsetDir + "/" + topicPartition.partition)._1
    offsetString match {
      case Some(offsetStr) => (topicPartition, OffsetMetadataAndError(offsetStr.toLong))
      case None => (topicPartition, OffsetMetadataAndError.NoOffset)
    }
  }

  private def fetchOffsets(partitions: Seq[TopicAndPartition]) = {
    if (partitions.isEmpty)
      Some(OffsetFetchResponse(Map.empty))
    else if (config.offsetsStorage == "zookeeper") {
      val offsets = partitions.map(fetchOffsetFromZooKeeper)
      Some(OffsetFetchResponse(immutable.Map(offsets:_*)))
    } else {
      val offsetFetchRequest = OffsetFetchRequest(groupId = config.groupId, requestInfo = partitions, clientId = config.clientId)

      var offsetFetchResponseOpt: Option[OffsetFetchResponse] = None
      while (!isShuttingDown.get && !offsetFetchResponseOpt.isDefined) {
        offsetFetchResponseOpt = offsetsChannelLock synchronized {
          ensureOffsetManagerConnected()
          try {
            offsetsChannel.send(offsetFetchRequest)
            val offsetFetchResponse = OffsetFetchResponse.readFrom(offsetsChannel.receive().payload())
            trace("Offset fetch response: %s.".format(offsetFetchResponse))

            val (leaderChanged, loadInProgress) =
              offsetFetchResponse.requestInfo.values.foldLeft(false, false) { case (folded, offsetMetadataAndError) =>
                (folded._1 || (offsetMetadataAndError.error == Errors.NOT_COORDINATOR_FOR_GROUP.code),
                 folded._2 || (offsetMetadataAndError.error == Errors.GROUP_LOAD_IN_PROGRESS.code))
              }

            if (leaderChanged) {
              offsetsChannel.disconnect()
              debug("Could not fetch offsets (because offset manager has moved).")
              None // retry
            }
            else if (loadInProgress) {
              debug("Could not fetch offsets (because offset cache is being loaded).")
              None // retry
            }
            else {
              if (config.dualCommitEnabled) {
                // if dual-commit is enabled (i.e., if a consumer group is migrating offsets to kafka), then pick the
                // maximum between offsets in zookeeper and kafka.
                val kafkaOffsets = offsetFetchResponse.requestInfo
                val mostRecentOffsets = kafkaOffsets.map { case (topicPartition, kafkaOffset) =>
                  val zkOffset = fetchOffsetFromZooKeeper(topicPartition)._2.offset
                  val mostRecentOffset = zkOffset.max(kafkaOffset.offset)
                  (topicPartition, OffsetMetadataAndError(mostRecentOffset, kafkaOffset.metadata, Errors.NONE.code))
                }
                Some(OffsetFetchResponse(mostRecentOffsets))
              }
              else
                Some(offsetFetchResponse)
            }
          }
          catch {
            case e: Exception =>
              warn("Error while fetching offsets from %s:%d. Possible cause: %s".format(offsetsChannel.host, offsetsChannel.port, e.getMessage))
              offsetsChannel.disconnect()
              None // retry
          }
        }

        if (offsetFetchResponseOpt.isEmpty) {
          debug("Retrying offset fetch in %d ms".format(config.offsetsChannelBackoffMs))
          Thread.sleep(config.offsetsChannelBackoffMs)
        }
      }

      offsetFetchResponseOpt
    }
  }


  class ZKSessionExpireListener(val dirs: ZKGroupDirs,
                                 val consumerIdString: String,
                                 val topicCount: TopicCount,
                                 val loadBalancerListener: ZKRebalancerListener)
    extends IZkStateListener {
    @throws[Exception]
    def handleStateChanged(state: KeeperState) {
      // do nothing, since zkclient will do reconnect for us.
    }

    /**
     * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
     * any ephemeral nodes here.
     *
     * @throws Exception
     *             On any error.
     */
    @throws[Exception]
    def handleNewSession() {
      /**
       *  When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has reestablished a
       *  connection for us. We need to release the ownership of the current consumer and re-register this
       *  consumer in the consumer registry and trigger a rebalance.
       */
      info("ZK expired; release old broker parition ownership; re-register consumer " + consumerIdString)
      loadBalancerListener.resetState()
      registerConsumerInZK(dirs, consumerIdString, topicCount)
      // explicitly trigger load balancing for this consumer
      loadBalancerListener.syncedRebalance()
      // There is no need to resubscribe to child and state changes.
      // The child change watchers will be set inside rebalance when we read the children list.
    }

    override def handleSessionEstablishmentError(error: Throwable): Unit = {
      fatal("Could not establish session with zookeeper", error)
    }
  }

  class ZKTopicPartitionChangeListener(val loadBalancerListener: ZKRebalancerListener)
    extends IZkDataListener {

    def handleDataChange(dataPath : String, data: Object) {
      try {
        info("Topic info for path " + dataPath + " changed to " + data.toString + ", triggering rebalance")
        // queue up the rebalance event
        loadBalancerListener.rebalanceEventTriggered()
        // There is no need to re-subscribe the watcher since it will be automatically
        // re-registered upon firing of this event by zkClient
      } catch {
        case e: Throwable => error("Error while handling topic partition change for data path " + dataPath, e )
      }
    }

    @throws[Exception]
    def handleDataDeleted(dataPath : String) {
      // TODO: This need to be implemented when we support delete topic
      warn("Topic for path " + dataPath + " gets deleted, which should not happen at this time")
    }
  }

  class ZKRebalancerListener(val group: String, val consumerIdString: String,
                             val kafkaMessageAndMetadataStreams: mutable.Map[String,List[KafkaStream[_,_]]])
    extends IZkChildListener {

    private val partitionAssignor = PartitionAssignor.createInstance(config.partitionAssignmentStrategy)

    private var isWatcherTriggered = false
    private val lock = new ReentrantLock
    private val cond = lock.newCondition()

    @volatile private var allTopicsOwnedPartitionsCount = 0
    newGauge("OwnedPartitionsCount",
      new Gauge[Int] {
        def value() = allTopicsOwnedPartitionsCount
      },
      Map("clientId" -> config.clientId, "groupId" -> config.groupId))

    private def ownedPartitionsCountMetricTags(topic: String) = Map("clientId" -> config.clientId, "groupId" -> config.groupId, "topic" -> topic)

    private val watcherExecutorThread = new Thread(consumerIdString + "_watcher_executor") {
      override def run() {
        info("starting watcher executor thread for consumer " + consumerIdString)
        var doRebalance = false
        while (!isShuttingDown.get) {
          try {
            lock.lock()
            try {
              if (!isWatcherTriggered)
                cond.await(1000, TimeUnit.MILLISECONDS) // wake up periodically so that it can check the shutdown flag
            } finally {
              doRebalance = isWatcherTriggered
              isWatcherTriggered = false
              lock.unlock()
            }
            if (doRebalance)
              syncedRebalance
          } catch {
            case t: Throwable => error("error during syncedRebalance", t)
          }
        }
        info("stopping watcher executor thread for consumer " + consumerIdString)
      }
    }
    watcherExecutorThread.start()

    @throws[Exception]
    def handleChildChange(parentPath : String, curChilds : java.util.List[String]) {
      rebalanceEventTriggered()
    }

    def rebalanceEventTriggered() {
      inLock(lock) {
        isWatcherTriggered = true
        cond.signalAll()
      }
    }

    private def deletePartitionOwnershipFromZK(topic: String, partition: Int) {
      val topicDirs = new ZKGroupTopicDirs(group, topic)
      val znode = topicDirs.consumerOwnerDir + "/" + partition
      zkUtils.deletePath(znode)
      debug("Consumer " + consumerIdString + " releasing " + znode)
    }

    /**
      * 分区的所有权记录在ZK的/consumers/[group_id]/owner/[topic]/[partition_id] → consumer_thread_id 节点，
      * 表示“主题一分区”会被指定的消费者线程所消费，或者说分区被分配给消费者,消费者拥有了分区
      */
    private def releasePartitionOwnership(localTopicRegistry: Pool[String, Pool[Int, PartitionTopicInfo]])= {
      info("Releasing partition ownership")
      for ((topic, infos) <- localTopicRegistry) {
        for(partition <- infos.keys) {
          deletePartitionOwnershipFromZK(topic, partition) //删除ZK中的分区所有权记录
        }
        removeMetric("OwnedPartitionsCount", ownedPartitionsCountMetricTags(topic))
        localTopicRegistry.remove(topic)
      }
      allTopicsOwnedPartitionsCount = 0
    }

    def resetState() {
      topicRegistry.clear
    }

    def syncedRebalance() {
      rebalanceLock synchronized {
        rebalanceTimer.time {
          for (i <- 0 until config.rebalanceMaxRetries) {
            if(isShuttingDown.get())  {
              return
            }
            info("begin rebalancing consumer " + consumerIdString + " try #" + i)
            var done = false
            var cluster: Cluster = null
            try {
              cluster = zkUtils.getCluster()
              done = rebalance(cluster)
            } catch {
              case e: Throwable =>
                /** occasionally, we may hit a ZK exception because the ZK state is changing while we are iterating.
                  * For example, a ZK node can disappear between the time we get all children and the time we try to get
                  * the value of a child. Just let this go since another rebalance will be triggered.
                  **/
                info("exception during rebalance ", e)
            }
            info("end rebalancing consumer " + consumerIdString + " try #" + i)
            if (done) {
              return
            } else {
              /* Here the cache is at a risk of being stale. To take future rebalancing decisions correctly, we should
               * clear the cache */
              info("Rebalancing attempt failed. Clearing the cache before the next rebalancing operation is triggered")
            }
            // stop all fetchers and clear all the queues to avoid data duplication
            closeFetchersForQueues(cluster, kafkaMessageAndMetadataStreams, topicThreadIdAndQueues.map(q => q._2))
            Thread.sleep(config.rebalanceBackoffMs)
          }
        }
      }

      throw new ConsumerRebalanceFailedException(consumerIdString + " can't rebalance after " + config.rebalanceMaxRetries +" retries")
    }

    private def rebalance(cluster: Cluster): Boolean = {
      val myTopicThreadIdsMap = TopicCount.constructTopicCount(
        group, consumerIdString, zkUtils, config.excludeInternalTopics).getConsumerThreadIdsPerTopic
      val brokers = zkUtils.getAllBrokersInCluster()
      if (brokers.size == 0) { //启动消费者时kafka集群还没启动,很少见,会打日志并注册子变动,这样broker启动时会重新触发再平衡
        // This can happen in a rare case when there are no brokers available in the cluster when the consumer is started.
        // We log an warning and register for child changes on brokers/id so that rebalance can be triggered when the brokers
        // are up.
        warn("no brokers found when trying to rebalance.")
        zkUtils.zkClient.subscribeChildChanges(BrokerIdsPath, loadBalancerListener)
        true
      }
      else {
        /**
         * fetchers must be stopped to avoid data duplication, since if the current
         * rebalancing attempt fails, the partitions that are released could be owned by another consumer.
         * But if we don't stop the fetchers first, this consumer would continue returning data for released
         * partitions in parallel. So, not stopping the fetchers leads to duplicate data.
         */
        //1).停止拉去线程,防止数据重复
        closeFetchers(cluster, kafkaMessageAndMetadataStreams, myTopicThreadIdsMap)
        if (consumerRebalanceListener != null) {
          info("Invoking rebalance listener before relasing partition ownerships.")
          consumerRebalanceListener.beforeReleasingPartitions(
            if (topicRegistry.size == 0)
              new java.util.HashMap[String, java.util.Set[java.lang.Integer]]
            else
              topicRegistry.map(topics =>
                topics._1 -> topics._2.keys   // note this is incorrect, see KAFKA-2284
              ).toMap.asJava.asInstanceOf[java.util.Map[String, java.util.Set[java.lang.Integer]]]
          )
        }
        //释放topicRegistry中分区的所有者
        releasePartitionOwnership(topicRegistry)
        val assignmentContext = new AssignmentContext(group, consumerIdString, config.excludeInternalTopics, zkUtils)//代表分区分配的上下文对象,提供两个变量:所有的分区(partitionsForTopic)、所有的消费者线程 (consumersForTopic)，
        val globalPartitionAssignment = partitionAssignor.assign(assignmentContext) //计算得到所有消费者的分配结果
        val partitionAssignment = globalPartitionAssignment.get(assignmentContext.consumerId) //属于当前消费者的分配context
        val currentTopicRegistry = new Pool[String, Pool[Int, PartitionTopicInfo]](
          valueFactory = Some((_: String) => new Pool[Int, PartitionTopicInfo]))

        // fetch current offsets for all topic-partitions
        //分配给当前消费者的所有分区
        val topicPartitions = partitionAssignment.keySet.toSeq
        //当前消费者所有分区的偏移量
        val offsetFetchResponseOpt = fetchOffsets(topicPartitions)

        if (isShuttingDown.get || !offsetFetchResponseOpt.isDefined)
          false
        else {
          val offsetFetchResponse = offsetFetchResponseOpt.get
          //当前消费者的主题注册信息加入到currentTopicRegistry中(双层map)
          topicPartitions.foreach(topicAndPartition => {
            val (topic, partition) = topicAndPartition.asTuple
            val offset = offsetFetchResponse.requestInfo(topicAndPartition).offset
            val threadId = partitionAssignment(topicAndPartition)
            addPartitionTopicInfo(currentTopicRegistry, partition, topic, offset, threadId)
          })

          /**
           * move the partition ownership here, since that can be used to indicate a truly successful re-balancing attempt
           * A rebalancing attempt is completed successfully only after the fetchers have been started correctly
            * 为分区重新分配消费者
           */
          if(reflectPartitionOwnershipDecision(partitionAssignment)) {
            allTopicsOwnedPartitionsCount = partitionAssignment.size

            partitionAssignment.view.groupBy { case (topicPartition, _) => topicPartition.topic }
                                      .foreach { case (topic, partitionThreadPairs) =>
              newGauge("OwnedPartitionsCount",
                new Gauge[Int] {
                  def value() = partitionThreadPairs.size
                },
                ownedPartitionsCountMetricTags(topic))
            }

            topicRegistry = currentTopicRegistry //前面addPartitionTopicInfo()方法修改的
            // Invoke beforeStartingFetchers callback if the consumerRebalanceListener is set.
            if (consumerRebalanceListener != null) {
              info("Invoking rebalance listener before starting fetchers.")

              // Partition assignor returns the global partition assignment organized as a map of [TopicPartition, ThreadId]
              // per consumer, and we need to re-organize it to a map of [Partition, ThreadId] per topic before passing
              // to the rebalance callback.
              val partitionAssginmentGroupByTopic = globalPartitionAssignment.values.flatten.groupBy[String] {
                case (topicPartition, _) => topicPartition.topic
              }
              val partitionAssigmentMapForCallback = partitionAssginmentGroupByTopic.map({
                case (topic, partitionOwnerShips) =>
                  val partitionOwnershipForTopicScalaMap = partitionOwnerShips.map({
                    case (topicAndPartition, consumerThreadId) =>
                      (topicAndPartition.partition: Integer) -> consumerThreadId
                  }).toMap
                  topic -> partitionOwnershipForTopicScalaMap.asJava
              })
              consumerRebalanceListener.beforeStartingFetchers(
                consumerIdString,
                partitionAssigmentMapForCallback.asJava
              )
            }
            updateFetcher(cluster) //创建拉取线程
            true
          } else {
            false
          }
        }
      }
    }

    private def closeFetchersForQueues(cluster: Cluster,
                                       messageStreams: Map[String,List[KafkaStream[_,_]]],
                                       queuesToBeCleared: Iterable[BlockingQueue[FetchedDataChunk]]) {
      val allPartitionInfos = topicRegistry.values.map(p => p.values).flatten
      fetcher match {
        case Some(f) => //ConsumerFetcherManager 拉取线程管理器
          f.stopConnections //关闭拉取线程管理器,会关闭它管理的所有拉取线程
          clearFetcherQueues(allPartitionInfos, cluster, queuesToBeCleared, messageStreams) //清空队列和消息流
          /**
          * here, we need to commit offsets before stopping the consumer from returning any more messages
          * from the current data chunk. Since partition ownership is not yet released, this commit offsets
          * call will ensure that the offsets committed now will be used by the next consumer thread owning the partition
          * for the current data chunk. Since the fetchers are already shutdown and this is the last chunk to be iterated
          * by the consumer, there will be no more messages returned by this iterator until the rebalancing finishes
          * successfully and the fetchers restart to fetch more data chunks
          **/
        if (config.autoCommitEnable) {
          info("Committing all offsets after clearing the fetcher queues")
          commitOffsets(true) //提交偏移量
        }
        case None =>
      }
    }

    private def clearFetcherQueues(topicInfos: Iterable[PartitionTopicInfo], cluster: Cluster,
                                   queuesTobeCleared: Iterable[BlockingQueue[FetchedDataChunk]],
                                   messageStreams: Map[String,List[KafkaStream[_,_]]]) {

      // Clear all but the currently iterated upon chunk in the consumer thread's queue
      queuesTobeCleared.foreach(_.clear)
      info("Cleared all relevant queues for this fetcher")

      // Also clear the currently iterated upon chunk in the consumer threads
      if(messageStreams != null)
         messageStreams.foreach(_._2.foreach(s => s.clear()))

      info("Cleared the data chunks in all the consumer message iterators")

    }

    private def closeFetchers(cluster: Cluster, messageStreams: Map[String,List[KafkaStream[_,_]]],
                              relevantTopicThreadIdsMap: Map[String, Set[ConsumerThreadId]]) {
      // only clear the fetcher queues for certain topic partitions that *might* no longer be served by this consumer
      // after this rebalancing attempt
      val queuesTobeCleared = topicThreadIdAndQueues.filter(q => relevantTopicThreadIdsMap.contains(q._1._1)).map(q => q._2)
      closeFetchersForQueues(cluster, messageStreams, queuesTobeCleared)
    }

    //再平衡后,通过拉去线程管理器启动拉取线程
    private def updateFetcher(cluster: Cluster) {
      // update partitions for fetcher
      var allPartitionInfos : List[PartitionTopicInfo] = Nil
      for (partitionInfos <- topicRegistry.values)
        for (partition <- partitionInfos.values)
          allPartitionInfos ::= partition
      info("Consumer " + consumerIdString + " selected partitions : " +
        allPartitionInfos.sortWith((s,t) => s.partitionId < t.partitionId).map(_.toString).mkString(","))

      fetcher match {
        case Some(f) =>
          f.startConnections(allPartitionInfos, cluster)
        case None =>
      }
    }

    //根据partitionAssignment重建分区的所有权
    private def reflectPartitionOwnershipDecision(partitionAssignment: Map[TopicAndPartition, ConsumerThreadId]): Boolean = {
      var successfullyOwnedPartitions : List[(String, Int)] = Nil
      val partitionOwnershipSuccessful = partitionAssignment.map { partitionOwner =>
        val topic = partitionOwner._1.topic
        val partition = partitionOwner._1.partition
        val consumerThreadId = partitionOwner._2
        val partitionOwnerPath = zkUtils.getConsumerPartitionOwnerPath(group, topic, partition)
        try {
          zkUtils.createEphemeralPathExpectConflict(partitionOwnerPath, consumerThreadId.toString)//去ZK对应目录增加节点
          info(consumerThreadId + " successfully owned partition " + partition + " for topic " + topic)
          successfullyOwnedPartitions ::= (topic, partition)
          true
        } catch {
          case _: ZkNodeExistsException =>
            // The node hasn't been deleted by the original owner. So wait a bit and retry.
            info("waiting for the partition ownership to be deleted: " + partition + " for topic " + topic)
            false
        }
      }
      val hasPartitionOwnershipFailed = partitionOwnershipSuccessful.foldLeft(0)((sum, decision) => sum + (if(decision) 0 else 1))
      /* even if one of the partition ownership attempt has failed, return false */
      if(hasPartitionOwnershipFailed > 0) {
        // remove all paths that we have owned in ZK
        successfullyOwnedPartitions.foreach(topicAndPartition => deletePartitionOwnershipFromZK(topicAndPartition._1, topicAndPartition._2))
        false
      }
      else true
    }

    private def addPartitionTopicInfo(currentTopicRegistry: Pool[String, Pool[Int, PartitionTopicInfo]],
                                      partition: Int, topic: String,
                                      offset: Long, consumerThreadId: ConsumerThreadId) {
      val partTopicInfoMap = currentTopicRegistry.getAndMaybePut(topic)
      //每个线程有一个对应的队列
      val queue = topicThreadIdAndQueues.get((topic, consumerThreadId))
      val consumedOffset = new AtomicLong(offset)
      val fetchedOffset = new AtomicLong(offset)
      //从zk获取的偏移量作为已消费和已拉取的最近位置
      val partTopicInfo = new PartitionTopicInfo(topic,
                                                 partition,
                                                 queue,
                                                 consumedOffset,
                                                 fetchedOffset,
                                                 new AtomicInteger(config.fetchMessageMaxBytes),
                                                 config.clientId)
      partTopicInfoMap.put(partition, partTopicInfo)
      debug(partTopicInfo + " selected new offset " + offset)
      checkpointedZkOffsets.put(TopicAndPartition(topic, partition), offset)
    }
  }

  private def reinitializeConsumer[K,V](
      topicCount: TopicCount,
      queuesAndStreams: List[(LinkedBlockingQueue[FetchedDataChunk],KafkaStream[K,V])]) {

    val dirs = new ZKGroupDirs(config.groupId)

    // listener to consumer and partition changes
    if (loadBalancerListener == null) {
      val topicStreamsMap = new mutable.HashMap[String,List[KafkaStream[K,V]]]
      loadBalancerListener = new ZKRebalancerListener(
        config.groupId, consumerIdString, topicStreamsMap.asInstanceOf[scala.collection.mutable.Map[String, List[KafkaStream[_,_]]]])
    }

    // create listener for session expired event if not exist yet
    if (sessionExpirationListener == null)
      sessionExpirationListener = new ZKSessionExpireListener(
        dirs, consumerIdString, topicCount, loadBalancerListener)

    // create listener for topic partition change event if not exist yet
    if (topicPartitionChangeListener == null)
      topicPartitionChangeListener = new ZKTopicPartitionChangeListener(loadBalancerListener)

    val topicStreamsMap = loadBalancerListener.kafkaMessageAndMetadataStreams

    // map of {topic -> Set(thread-1, thread-2, ...)}
    val consumerThreadIdsPerTopic: Map[String, Set[ConsumerThreadId]] =
      topicCount.getConsumerThreadIdsPerTopic

    val allQueuesAndStreams = topicCount match {
      case _: WildcardTopicCount =>
        /*
         * Wild-card consumption streams share the same queues, so we need to
         * duplicate the list for th
         * e subsequent zip operation.
         */
        (1 to consumerThreadIdsPerTopic.keySet.size).flatMap(_ => queuesAndStreams).toList
      case _: StaticTopicCount =>
        queuesAndStreams
    }

    val topicThreadIds = consumerThreadIdsPerTopic.map { case (topic, threadIds) =>
      threadIds.map((topic, _))
    }.flatten

    require(topicThreadIds.size == allQueuesAndStreams.size,
      "Mismatch between thread ID count (%d) and queue count (%d)"
      .format(topicThreadIds.size, allQueuesAndStreams.size))
    val threadQueueStreamPairs = topicThreadIds.zip(allQueuesAndStreams)

    threadQueueStreamPairs.foreach(e => {
      val topicThreadId = e._1
      val q = e._2._1
      topicThreadIdAndQueues.put(topicThreadId, q)
      debug("Adding topicThreadId %s and queue %s to topicThreadIdAndQueues data structure".format(topicThreadId, q.toString))
      newGauge(
        "FetchQueueSize",
        new Gauge[Int] {
          def value = q.size
        },
        Map("clientId" -> config.clientId,
          "topic" -> topicThreadId._1,
          "threadId" -> topicThreadId._2.threadId.toString)
      )
    })

    val groupedByTopic = threadQueueStreamPairs.groupBy(_._1._1)
    groupedByTopic.foreach(e => {
      val topic = e._1
      val streams = e._2.map(_._2._2).toList
      topicStreamsMap += (topic -> streams)
      debug("adding topic %s and %d streams to map.".format(topic, streams.size))
    })

    // listener to consumer and partition changes
    zkUtils.zkClient.subscribeStateChanges(sessionExpirationListener)

    zkUtils.zkClient.subscribeChildChanges(dirs.consumerRegistryDir, loadBalancerListener)

    topicStreamsMap.foreach { topicAndStreams =>
      // register on broker partition path changes
      val topicPath = BrokerTopicsPath + "/" + topicAndStreams._1
      zkUtils.zkClient.subscribeDataChanges(topicPath, topicPartitionChangeListener)
    }

    // explicitly trigger load balancing for this consumer
    //重新初始化消费者时,直接强制触发一次再平衡
    loadBalancerListener.syncedRebalance()
  }

  class WildcardStreamsHandler[K,V](topicFilter: TopicFilter,
                                  numStreams: Int,
                                  keyDecoder: Decoder[K],
                                  valueDecoder: Decoder[V])
                                extends TopicEventHandler[String] {

    if (messageStreamCreated.getAndSet(true))
      throw new RuntimeException("Each consumer connector can create " +
        "message streams by filter at most once.")

    private val wildcardQueuesAndStreams = (1 to numStreams)
      .map(_ => {
        val queue = new LinkedBlockingQueue[FetchedDataChunk](config.queuedMaxMessages)
        val stream = new KafkaStream[K,V](queue,
                                          config.consumerTimeoutMs,
                                          keyDecoder,
                                          valueDecoder,
                                          config.clientId)
        (queue, stream)
    }).toList

     // bootstrap with existing topics
    private var wildcardTopics =
      zkUtils.getChildrenParentMayNotExist(BrokerTopicsPath)
        .filter(topic => topicFilter.isTopicAllowed(topic, config.excludeInternalTopics))

    private val wildcardTopicCount = TopicCount.constructTopicCount(
      consumerIdString, topicFilter, numStreams, zkUtils, config.excludeInternalTopics)

    val dirs = new ZKGroupDirs(config.groupId)
    registerConsumerInZK(dirs, consumerIdString, wildcardTopicCount)
    reinitializeConsumer(wildcardTopicCount, wildcardQueuesAndStreams)

    /*
     * Topic events will trigger subsequent synced rebalances.
     */
    info("Creating topic event watcher for topics " + topicFilter)
    wildcardTopicWatcher = new ZookeeperTopicEventWatcher(zkUtils, this)

    def handleTopicEvent(allTopics: Seq[String]) {
      debug("Handling topic event")

      val updatedTopics = allTopics.filter(topic => topicFilter.isTopicAllowed(topic, config.excludeInternalTopics))

      val addedTopics = updatedTopics filterNot (wildcardTopics contains)
      if (addedTopics.nonEmpty)
        info("Topic event: added topics = %s"
                             .format(addedTopics))

      /*
       * TODO: Deleted topics are interesting (and will not be a concern until
       * 0.8 release). We may need to remove these topics from the rebalance
       * listener's map in reinitializeConsumer.
       */
      val deletedTopics = wildcardTopics filterNot (updatedTopics contains)
      if (deletedTopics.nonEmpty)
        info("Topic event: deleted topics = %s"
                             .format(deletedTopics))

      wildcardTopics = updatedTopics
      info("Topics to consume = %s".format(wildcardTopics))

      if (addedTopics.nonEmpty || deletedTopics.nonEmpty)
        reinitializeConsumer(wildcardTopicCount, wildcardQueuesAndStreams)
    }

    def streams: Seq[KafkaStream[K,V]] =
      wildcardQueuesAndStreams.map(_._2)
  }
}
