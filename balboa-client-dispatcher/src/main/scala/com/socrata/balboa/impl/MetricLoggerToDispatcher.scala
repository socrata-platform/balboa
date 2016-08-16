package com.socrata.balboa.impl

import com.socrata.balboa.config.JMSClientConfig
import com.socrata.metrics.collection.LinkedBlockingPreBufferQueue
import com.socrata.metrics.components._
import com.typesafe.config.Config

/**
 * Metric Logger that internally uses a dispatcher for sending metrics
 * to multiple endpoints.
 */
trait MetricLoggerToDispatcher extends BaseMetricLoggerComponent {

  def config: Config

  // scalastyle:off method.name

  /**
   * NOTE: This method is being replaced with [[MetricLogger()]].
   */
  @deprecated("Use MetricLogger", "v0.15.1 (2015-04-15)")
  override def MetricLogger(serverName: String, queueName: String, backupFileName: String): MetricLogger =
    MetricLogger()

  /**
   * Creates a Metric Logger using preset configurations.
   *
   * @return [[MetricLoggerLike]] instance.
   */
  override def MetricLogger(): MetricLogger = new MetricLogger() with MetricEnqueuer
    with MetricDequeuerService
    with HashMapBufferComponent
    with BalboaDispatcherComponent
    with LinkedBlockingPreBufferQueue
    with BufferedStreamEmergencyWriterComponent
    with DispatcherInformation {

    /**
      * See [[DispatcherInformation.components]].
      */
    override lazy val components: Iterable[MessageQueueComponent] = {
      List(new ActiveMQueueComponent()
        with ConfiguredServerInformation
        with BufferedStreamEmergencyWriterComponent {
        override val jmsClientConfig: JMSClientConfig = new JMSClientConfig(config)
      })
    }
  }
}
