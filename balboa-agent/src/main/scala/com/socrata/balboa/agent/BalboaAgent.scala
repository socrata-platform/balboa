package com.socrata.balboa.agent

import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}

import com.blist.metrics.impl.queue.MetricJmsQueueNotSingleton
import com.codahale.metrics.JmxReporter
import com.socrata.balboa.agent.metrics.BalboaAgentMetrics
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.activemq.ActiveMQConnection

/**
  * Balboa Agent class serves as the entry point for running the existing application.
  *
  * This process sets up a scheduler to run at a fixed interval, consuming metrics for a directory, and
  * emitting them to an Activemq Server.
  */
object BalboaAgent extends App with Config with StrictLogging {

  private val scheduler = Executors.newScheduledThreadPool(1)
  private val INTERRUPT_EXISTING_CONSUMER = false
  private val dataDir = dataDirectory()
  private val dirSizeMetric = BalboaAgentMetrics.directorySize("data-dir", dataDir)
  private val jmxReporter = JmxReporter.forRegistry(BalboaAgentMetrics.registry).build()
  jmxReporter.start()

  private val additionalInfo = (activemqUser, activemqPassword) match {
    case (Some(user), Some(password)) => s"AMQ User: $user"
    case _ => ""
  }

  logger info s"Starting Balboa Agent.  Consuming metrics from ${dataDir.getAbsolutePath}.  " +
    s"AMQ Server: $activemqServer, AMQ Queue: $activemqQueue $additionalInfo".trim

  logger.debug("Attempting to create Activemq Connection")
  val amqConnection = (activemqUser, activemqPassword) match {
    case (Some(user), Some(password)) => ActiveMQConnection.makeConnection(user, password, activemqServer)
    case _ => ActiveMQConnection.makeConnection(activemqServer)
  }
  logger.debug(s"Created Activemq Connection $amqConnection")

  // TODO (Pre req: Java 8) Use Java Duration class.
  val future: ScheduledFuture[_] = scheduler.scheduleWithFixedDelay(new Runnable {

    override def run(): Unit = {
      val mc = new MetricConsumer(dataDir, new MetricJmsQueueNotSingleton(amqConnection, activemqQueue))
      try {
        logger.debug(s"Attempting to run Metric Consumer $mc")
        mc.run() // Recursively reads all metric data files and writes them to a queue.
      } finally {
        logger.debug(s"Attempting to close Metric Consumer $mc")
        mc.close() // Release all associated resources.
      }
    }
  }, initialDelay(), interval(), TimeUnit.MILLISECONDS)

  Runtime.getRuntime.addShutdownHook(new Thread() {

    override def run(): Unit = {
      logger info "Attempting to shut down Balboa Agent.  Attempt to cancel current and future runs."
      future.cancel(INTERRUPT_EXISTING_CONSUMER)
      jmxReporter.stop()
    }
  })

}
