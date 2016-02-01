package com.socrata.balboa.agent

import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}

import com.blist.metrics.impl.queue.MetricJmsQueueNotSingleton
import com.codahale.metrics.JmxReporter
import com.socrata.balboa.agent.metrics.BalboaAgentMetrics
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.activemq.ActiveMQConnection

object BalboaAgent extends App with Config with LazyLogging {

  /**
   * Create a Report for metrics to reported via JMX.
   */
  private val scheduler = Executors.newScheduledThreadPool(1)

  /**
   * Is it appropiate to stop an existing Metric Consumer task???
   */
  private val INTERRUPT_EXISTING_CONSUMER = false

  val dataDir = dataDirectory()

  // TODO Ugh has side effects
  val dirSizeMetric = BalboaAgentMetrics.directorySize("data-dir", dataDir)

  // Start the JMX Reporter
  val jmxReporter = JmxReporter.forRegistry(BalboaAgentMetrics.registry).build()
  jmxReporter.start()

  val additionalInfo = (activemqUser, activemqPassword) match {
    case (Some(user), Some(password)) => s"AMQ User: $user"
    case _ => ""
  }

  /**
   * TODO Replace Metric Consumer with something less prone to errors.
   */
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
      // Wrapping the Metric Consumer inside an anonymous inner function makes the metric consumer more ephemeral
      // This will require a little more work from the JVM to create and cleanup but that is what it is designed to do.
      val mc = new MetricConsumer(dataDir, new MetricJmsQueueNotSingleton(amqConnection, activemqQueue))
      logger.debug(s"Attempting to run Metric Consumer $mc")
      mc.run()
      mc.close() // Ensure the Metrics Consumer cleans up after itself.
    }
  }, initialDelay(), interval(), TimeUnit.MILLISECONDS)

  Runtime.getRuntime.addShutdownHook(new Thread() {

    override def run(): Unit = {
      logger info "Attempting to shut down Balboa Agent.  Attempt to cancel and future subsequent runs."
      future.cancel(INTERRUPT_EXISTING_CONSUMER)
      jmxReporter.stop()
    }
  })

}
