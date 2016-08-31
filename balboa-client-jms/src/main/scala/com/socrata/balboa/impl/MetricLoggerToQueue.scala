package com.socrata.balboa.impl

import java.io.File
import java.nio.file.Paths

import com.socrata.balboa.config.JMSClientConfig
import com.socrata.metrics.collection.LinkedBlockingPreBufferQueue
import com.socrata.metrics.components.{BaseMetricLoggerComponent, MetricEnqueuer}
import com.typesafe.config.ConfigFactory

trait ServerInformation {
  def activeServer:String
  def activeQueue:String
  def backupFile:File
}

trait ConfiguredServerInformation extends ServerInformation {
  val jmsClientConfig: JMSClientConfig
  override def activeServer: String = jmsClientConfig.activemqServer
  override lazy val activeQueue = jmsClientConfig.activemqQueue
  override lazy val backupFile = jmsClientConfig.emergencyBackUpFile("jms")
}

trait LoadJMSClientConfig {
  val jmsClientConfig = new JMSClientConfig(ConfigFactory.load())
}

trait MetricLoggerToQueue extends BaseMetricLoggerComponent {

  // scalastyle:off method.name

  def MetricLogger(serverName:String, queueName:String, backupFileName:String):MetricLogger = { // scalastyle:ignore
    new MetricLogger() with MetricEnqueuer
                       with MetricDequeuerService
                       with HashMapBufferComponent
                       with ActiveMQueueComponent
                       with LinkedBlockingPreBufferQueue
                       with BufferedStreamEmergencyWriterComponent
                       with ServerInformation { override lazy val activeServer = serverName
                                                override lazy val activeQueue = queueName
                                                override lazy val backupFile = Paths.get(backupFileName).toFile}
  }

  /**
   * Creates a Metric Logger using a client defined set of parameters.
   *
   * @return [[MetricLoggerLike]] instance.
   */
  override def MetricLogger(): MetricLogger = new MetricLogger() // scalastyle:ignore
    with LoadJMSClientConfig
    with MetricEnqueuer
    with MetricDequeuerService
    with HashMapBufferComponent
    with ActiveMQueueComponent
    with LinkedBlockingPreBufferQueue
    with BufferedStreamEmergencyWriterComponent
    with ConfiguredServerInformation
}





