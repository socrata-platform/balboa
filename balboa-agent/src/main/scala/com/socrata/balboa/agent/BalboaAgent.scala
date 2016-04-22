package com.socrata.balboa.agent

import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}

import com.blist.metrics.impl.queue.MetricJmsQueueNotSingleton
import com.codahale.metrics.JmxReporter
import com.socrata.balboa.agent.metrics.BalboaAgentMetrics
import com.socrata.balboa.util.FileUtils
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.activemq.ActiveMQConnection

/**
  * Balboa Agent class serves as the entry point for running the existing
  * application.
  *
  * This process sets up a scheduler to run at a fixed interval, read metric
  * files from a directory, and send the metrics in the files to an ActiveMQ
  * server.
  */
object BalboaAgent extends App with Config with StrictLogging {
  logger info "Starting balboa-agent."
  private val scheduler = Executors.newScheduledThreadPool(1)
  private val dataDir = dataDirectory()
  BalboaAgentMetrics.numFiles("data", dataDir, Some(FileUtils.isBalboaDataFile))
  BalboaAgentMetrics.numFiles("broken", dataDir, Some(FileUtils.isBalboaBrokenFile))
  BalboaAgentMetrics.numFiles("lock", dataDir, Some(FileUtils.isBalboaLockFile))
  BalboaAgentMetrics.numFiles("immutable", dataDir, Some(FileUtils.isBalboaImmutableFile))
  private val jmxReporter = JmxReporter.forRegistry(BalboaAgentMetrics.registry).build()
  logger info "Starting the JMX Reporter."
  jmxReporter.start()

  logger info "Initializing ActiveMQ connection. (This is setting the username, password and destination.)"
  // An ActiveAMQConnection already contains the logic to do exponential backoff
  // and retry on failed connections. It will also automatically apply the same
  // logic to reconnecting to a server when the connection is lost.
  val amqConnection: ActiveMQConnection = try {
    (activemqUser, activemqPassword) match {
      case (Some(user), Some(password)) => ActiveMQConnection.makeConnection(user, password, activemqServer)
      case _                            => ActiveMQConnection.makeConnection(activemqServer)
    }
  } catch {
    case e: Throwable =>
      logger error("Unable to initialize ActiveMQ connection.", e)
      sys.exit(1)
  }

  logger info s"Initialized ActiveMQ connection ${amqConnection.getConnectionInfo}"

  logger info "Connecting to ActiveMQ broker. (This is the actual TCP connection.)"
  try {
    // This step is where the actual TCP connection is initiated. Everything up
    // until now was basically making some objects. The 'start' method may never
    // return if the destination server is unavailable. As far as I can tell from
    // testing, this method contains the logic to retry failed connections. It
    // has a backup built in, and once it gets up to a 30s interval, it just sits
    // and retries connections every 30s for at least 20min. I didn't test any
    // longer than that.
    amqConnection.start()
  } catch {
    case e: Throwable =>
      logger error("Unable to connect to ActiveMQ broker.", e)
      sys.exit(1)
  }
  logger info s"Connected to ActiveMQ broker '${amqConnection.getBrokerInfo}'."

  logger info s"Reading metrics from directory '${dataDir.getAbsolutePath}'."
  logger info s"Sending metrics to ActiveMQ queue '$activemqQueue'"

  val future: ScheduledFuture[_] = scheduler.scheduleWithFixedDelay(new Runnable {
    override def run(): Unit = {
      val mc = new MetricConsumer(dataDir, new MetricJmsQueueNotSingleton(amqConnection, activemqQueue))
      try {
        logger debug s"Attempting to run Metric Consumer $mc"
        mc.run() // Recursively reads all metric data files and writes them to a queue.
      } finally {
        logger debug s"Attempting to close Metric Consumer $mc"
        mc.close() // Release all associated resources.
      }
    }
  }, initialDelay(), interval(), TimeUnit.MILLISECONDS)

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      logger info "balboa-agent is shutting down."
      logger info "Canceling current and future runs."
      future.cancel(false)
      logger info "Stopping the JMX Reporter."
      jmxReporter.stop()
    }
  })

}
