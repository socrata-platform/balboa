package com.socrata.balboa.agent

import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}

import com.blist.metrics.impl.queue.MetricJmsQueue
import com.codahale.metrics.JmxReporter
import com.socrata.metrics.MetricQueue
import com.typesafe.scalalogging.slf4j.LazyLogging

object BalboaAgent extends App with Config with LazyLogging {

  /**
   * Create a Report for metrics to reported via JMX.
   */
  private val scheduler = Executors.newScheduledThreadPool(1)

  /**
   * The initial delay of the scheduled periodic task.
   */
  private val INITIAL_DELAY: Long = 0

  /**
   * Is it appropiate to stop an existing Metric Consumer task???
   */
  private val INTERRUPT_EXISTING_CONSUMER = false

  logger info "Loading Balboa Agent Configuration!"

  var dataDir = dataDirectory()

  // TODO Sleep time should not defaulted to Aggregate Granularity
  // TODO Rename sleep time to period.
  // This was copied from the original implementation.
  var period = sleepTime(MetricQueue.AGGREGATE_GRANULARITY)

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

  val mc = (activemqUser, activemqPassword) match {
    case (Some(user), Some(password)) => new MetricConsumer(
      dataDir, MetricJmsQueue.getInstance(user, password, activemqServer, activemqQueue))
    case _ => new MetricConsumer(dataDir, MetricJmsQueue.getInstance(activemqServer, activemqQueue))
  }
  val future: ScheduledFuture[_] = scheduler.scheduleWithFixedDelay(mc, INITIAL_DELAY, period, TimeUnit.MILLISECONDS)

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      future.cancel(INTERRUPT_EXISTING_CONSUMER)
      jmxReporter.stop()
    }
  })

}
