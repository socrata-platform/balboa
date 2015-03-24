package com.socrata.metrics.impl

import com.socrata.balboa.common.kafka.util.AddressAndPort
import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.metrics.collection.LinkedBlockingPreBufferQueue
import com.socrata.metrics.components.{MetricEnqueuer, MetricEntry, MetricLoggerComponent}
import org.apache.commons.logging.LogFactory

/**
 * Information that pertains to how to setup and configure producers that will communicate with Kafka.
 */
trait KafkaProducerInformation {

  /**
   * @return List of Kafka Brokers that exists within this environment.
   */
  def brokers: List[AddressAndPort]

  /**
   * @return The name of the topic that is being used.
   */
  def topic: String

  /**
   * @return The back up file where failed writes will go.
   */
  def emergencyBackUpFile: String

}

/**
 * Entry point for how to funnel Metric logs entries to Kafka.
 */
trait MetricLoggerToKafka extends MetricLoggerComponent {
  private val Log = LogFactory.getLog(classOf[MetricLogger])

  val delay = 120L
  val interval = 120L

  /**
   * Internal Metric Logger that is based off of [[MetricLogger]].
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
   * Produces a Metric Logger used to consume a dispatch metrics to a set of Kafka brokers.
   *
   * <br>
   * Note:
   * <ul>
   *   <li>This is an overridden method that uses different parameter names.</li>
   *   <li>The server list can be a subset of Kafka servers from a given cluster.  This list of servers is only used for
   *   identifying metadata.</li>
   * </ul>
   *
   * See: [[MetricLoggerComponent]].
   *
   * @param brokerList Comma separated list of "host:port" Kafka brokers.
   * @param topic Kafka topic to use.
   * @param emergencyFileName File where to put back up files
   * @return Logging component that is able to log metric messages.
   */
  override def MetricLogger(brokerList: String, topic: String, emergencyFileName: String): MetricLogger = {
    val t = topic
    val ebf = emergencyFileName
    new MetricLogger() with MetricEnqueuer
      with MetricDequeuerService
      with HashMapBufferComponent
      with BalboaKafkaComponent
      with LinkedBlockingPreBufferQueue
      with BufferedStreamEmergencyWriterComponent
      with KafkaProducerInformation {
      lazy val brokers = AddressAndPort.parse(brokerList)
      lazy val topic: String = t
      lazy val emergencyBackUpFile: String = ebf

    }
  }
}

/**
 * Used for a Java binding for [[MetricLoggerToKafka]].
 */
class MetricLoggerToKafkaJava extends MetricLoggerToKafka
