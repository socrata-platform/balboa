package com.socrata.balboa.agent

import java.io.File
import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}

import com.codahale.metrics.{CachedGauge, JmxReporter, MetricRegistry}
import com.socrata.balboa.agent.util.FileUtils
import com.socrata.metrics.MetricQueue
import com.typesafe.scalalogging.slf4j.Logger
import joptsimple.{OptionParser, OptionSet}
import org.slf4j.LoggerFactory

object CLIParamKeys {

  lazy val dataDir = "data-dir"
  lazy val sleepTime = "sleep-time"
  lazy val amqServer = "amq-server"
  lazy val amqQueue = "amq-queue"

}

object BalboaAgent extends App with Config {

  private lazy val logger = Logger(LoggerFactory getLogger this.getClass)

  /**
   * Place to register metrics that pertain specifically to Balboa Agent.
   */
  val metricRegistry = new MetricRegistry

  /**
   * Create a Report for metrics to reported via JMX.
   */
  val jmxReporter = JmxReporter.forRegistry(metricRegistry).build()

  private val scheduler = Executors.newScheduledThreadPool(1)

  /**
   * The initial delay of the scheduled periodic task.
   */
  private val INITIAL_DELAY: Long = 0

  /**
   * Is it appropiate to stop an existing Metric Consumer task.
   */
  private val INTERRUPT_EXISTING_CONSUMER = false
  logger info "Loading Balboa Agent Configuration!"

  var dataDir = dataDirectory(null)
  // TODO Sleep time should not defaulted to Aggregate Granularity
  // TODO Rename sleep time to period.
  var period = sleepTime(MetricQueue.AGGREGATE_GRANULARITY)
  var amqServer = activemqServer
  var amqQueue = activemqQueue

  val optParser: OptionParser = new OptionParser()
  // Can use a single configuration file for all command line application.
  val fileOpt = optParser.accepts(CLIParamKeys.dataDir, "Directory that contains Metrics Data.")
    .withRequiredArg()
    .ofType(classOf[File])
  val sleepOpt = optParser.accepts(CLIParamKeys.sleepTime, "Scheduled amount of time (ms) that the service will sleep before restarting.")
    .withRequiredArg()
    .ofType(classOf[Long])
  val amqServerOpt = optParser.accepts(CLIParamKeys.amqServer, "Active MQ Server to connect to.")
    .withRequiredArg()
    .ofType(classOf[String])
  val amqQueueOpt = optParser.accepts(CLIParamKeys.amqQueue, "Active MQ Queue to publish to.")
    .withRequiredArg()
    .ofType(classOf[String])

  // Overwrite properties with any Command Line Arguments.
  val set: OptionSet = optParser.parse(args: _*)
  set.valueOf(fileOpt) match {
    case d: File =>
      logger info s"Overwriting directory to ${d.getAbsolutePath}"
      dataDir = d
    case _ => // NOOP
  }
  if (set.has(sleepOpt)) {
    set.valueOf(sleepOpt) match {
      case time: Long =>
        logger info s"Overwriting sleep time to $time"
        period = time
    }
  }
  set.valueOf(amqServerOpt) match {
    case s: String =>
      logger info s"Overwriting AMQ Server to $s"
      amqServer = s
    case _ => // NOOP
  }
  set.valueOf(amqQueueOpt) match {
    case s: String =>
      logger info s"Overwriting AMQ Queue to $s"
      amqQueue = s
    case _ => // NOOP
  }

  metricRegistry.register(MetricRegistry.name(BalboaAgent.getClass, "data-dir", "size"),
    new CachedGauge[Int](5, TimeUnit.MINUTES) { //
      override def loadValue(): Int = FileUtils.getDirectories(dataDir).size()
    })

  // Start the JMX Reporter
  jmxReporter.start()

  /**
   * TODO Replace Metric Consumer with something less prone to errors.
   */
  logger info s"Starting Balboa Agent.  Consuming metrics from ${dataDir.getAbsolutePath}.  " +
    s"AMQ Server: ${amqServer}, AMQ Queue: ${amqQueue}"
  val future: ScheduledFuture[_] = scheduler.scheduleAtFixedRate(
    new MetricConsumer(dataDir, new ConsoleMetricQueue),
    INITIAL_DELAY, period, TimeUnit.MILLISECONDS)

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      future.cancel(INTERRUPT_EXISTING_CONSUMER)
      jmxReporter.stop()
    }
  })

}
