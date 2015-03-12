package com.socrata.metrics.impl


import com.socrata.balboa.kafka.util.AddressAndPort
import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.metrics.collection.LinkedBlockingPreBufferQueue
import com.socrata.metrics.components.{MetricEnqueuer, MetricEntry, MetricLoggerComponent}
import org.apache.commons.logging.LogFactory

/**
 * Information that pertains of how to setup and configure Kafka
 */
trait KafkaInformation {

  /**
   * @return List of Kafka Brokers that exists within this environment
   */
  def brokers: List[AddressAndPort]

  /**
   * @return The name of the topic that is being used.
   */
  def topic: String

  /**
   * @return The back up file where failed writes will go.
   */
  def backupFile: String

}

/**
 * Entry point for how to funnel Metric logs entries to Kafka.
 */
trait MetricLoggerToKafka extends MetricLoggerComponent {
  val delay = 120L
  val interval = 120L
  private val Log = LogFactory.getLog(classOf[MetricLogger])

  /**
   * Internal Metric Logger that is based off of [[MetricLogger]]
   */
  class MetricLogger extends MetricLoggerLike {
    self: MetricEnqueuer with MetricDequeuerService =>
    var acceptEnqueues = true
    val metricDequeuer = MetricDequeuer()
    val started = metricDequeuer.start(delay, interval)

    /** See [[MetricLoggerLike.logMetric()]] */
    override def logMetric(entityId: String, name: String, value: Number, timestamp: Long, recordType: RecordType): Unit = {
      if (acceptEnqueues)
        enqueue(MetricEntry(entityId, name, value, timestamp, recordType))
      else
        throw new IllegalStateException("MetricLogger has already been stopped")
    }

    /** See [[MetricLoggerLike.stop()]] */
    override def stop(): Unit = {
      acceptEnqueues = false
      Log.info("Beginning Balboa Kafka Client shutdown")
      metricDequeuer.stop()
    }
  }

  /**
   * Overridden method that uses different parameter names.
   *
   * See: [[MetricLoggerComponent]]
   *
   * @param serverList Comma separated list of server names.
   * @param topic Kafka topic to use.
   * @param backupFileName File where to put back up files
   * @return Logging component that is able to log metric messages.
   */
  override def MetricLogger(serverList: String, topic: String, backupFileName: String): MetricLogger =
    new MetricLogger() with MetricEnqueuer
      with MetricDequeuerService
      with HashMapBufferComponent
      with KafkaComponent
      with LinkedBlockingPreBufferQueue
      with BufferedStreamEmergencyWriterComponent
      with KafkaInformation {
      lazy val brokers = AddressAndPort.parse(serverList)
      lazy val topic: String = topic
      lazy val backupFile: String = backupFile
    }
}

/**
 * Java binding for [[MetricLoggerToKafka]].
 */
class MetricLoggerToKafkaJava extends MetricLoggerToKafka
