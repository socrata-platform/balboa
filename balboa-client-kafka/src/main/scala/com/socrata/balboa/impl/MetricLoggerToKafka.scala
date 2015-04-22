package com.socrata.balboa.impl

import java.io.File
import java.nio.file.Paths

import com.socrata.balboa.config.KafkaClientConfig
import com.socrata.balboa.metrics.util.AddressAndPort
import com.socrata.metrics.collection.LinkedBlockingPreBufferQueue
import com.socrata.metrics.components.{BaseMetricLoggerComponent, MetricEnqueuer, MetricLoggerComponent}



/**
 * Entry point for how to funnel Metric logs entries to Kafka.
 */
trait MetricLoggerToKafka extends BaseMetricLoggerComponent {

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
    // TODO Deprecate this method because it introduced argument dependencies into the client code.

    val t = topic
    val ebf = emergencyFileName
    new MetricLogger() with MetricEnqueuer
      with MetricDequeuerService
      with HashMapBufferComponent
      with BalboaKafkaComponent
      with LinkedBlockingPreBufferQueue
      with BufferedStreamEmergencyWriterComponent
      with KafkaProducerInformation {
      override lazy val brokers = AddressAndPort.parse(brokerList)
      override lazy val topic: String = t
      override lazy val file: File = Paths.get(ebf).toFile
    }
  }

  /**
   * Creates a Metric Logger To Kafka from using [[com.socrata.balboa.metrics.config.Configuration]].  This instantiator
   * exclusively relies on a properties file to extract a list of brokers.
   *
   * See [[MetricLoggerComponent.MetricLogger()]]
   */
  override def MetricLogger(): MetricLogger = new MetricLogger() with MetricEnqueuer
    with MetricDequeuerService
    with HashMapBufferComponent
    with BalboaKafkaComponent
    with LinkedBlockingPreBufferQueue
    with BufferedStreamEmergencyWriterComponent
    with ConfiguredKafkaProducerInfo
}

/**
 * Used for a Java binding for [[MetricLoggerToKafka]].
 */
class MetricLoggerToKafkaJava extends MetricLoggerToKafka
