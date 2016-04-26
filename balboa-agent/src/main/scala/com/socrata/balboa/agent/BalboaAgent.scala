package com.socrata.balboa.agent

import java.io.IOException
import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}
import javax.jms.{JMSException, ExceptionListener}

import com.blist.metrics.impl.queue.MetricJmsQueueNotSingleton
import com.codahale.metrics.JmxReporter
import com.socrata.balboa.agent.metrics.BalboaAgentMetrics
import com.socrata.balboa.util.FileUtils
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.activemq.transport.TransportListener
import org.apache.activemq.{ActiveMQConnection, ActiveMQConnectionFactory}

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

  logger info "Initializing ActiveMQ connection factory. (This is setting the username, password and destination.)"
  val amqConnectionFactory = (activemqUser, activemqPassword) match {
    case (Some(user), Some(password)) => new ActiveMQConnectionFactory(user, password, activemqServer)
    case _ => new ActiveMQConnectionFactory(activemqServer)
  }

  // The ActiveMQ libraries can be obscure when they are misbehaving. The hope
  // is that by registering this listener, additional information will be
  // written to the logs for troubleshooting.
  amqConnectionFactory.setTransportListener(new TransportListener {
    // The onCommand handler seems to be a routine handler. No additional error
    // reporting information is available by overriding it.
    override def onCommand(command: scala.Any): Unit = {}

    override def onException(error: IOException): Unit =
      logger error ("Problem with ActiveMQ connection.", error)

    // This method appears to be invoked when the library is first initializing
    // as well. So this log message will appear after the call to `.start` down
    // below.
    override def transportInterupted(): Unit =
      logger warn "ActiveMQ connection is being closed or has suffered an interruption from which it hopes to recover."

    override def transportResumed(): Unit =
      logger warn "ActiveMQ connection is starting up or has resumed after an interruption."
  })

  // An ActiveAMQConnection already contains the logic to do exponential backoff
  // and retry on failed connections. It will also automatically apply the same
  // logic to reconnecting to a server when the connection is lost.
  val amqConnection = try {
    amqConnectionFactory.createConnection().asInstanceOf[ActiveMQConnection]
  } catch {
    // Known causes of an exception here are poorly formatted connection string.
    // No amount of retry will fix that, so just exit.
    case e: Throwable =>
      logger error("Unable to initialize ActiveMQ connection.", e)
      sys.exit(1)
  }
  logger info s"Initialized ActiveMQ connection ${amqConnection.getConnectionInfo}"

  amqConnection.setExceptionListener(new ExceptionListener() {
    override def onException(exception: JMSException): Unit =
      logger error ("ActiveMQ connection encountered an error.", exception)
  })

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
    // It is unclear under what practical circumstances this exception can occur.
    // Network related connection failures do not appear to lead to an exception,
    // just infinite retry attempts within the AMQ libraries. Without
    // understanding the causes of this exception, it would be a bad idea to
    // have automatic retry of connection attempts that end in an exception.
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
      } catch {
        case e: Exception => logger.error("Metric consumer failed", e)
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

      // It seems like these calls would be useful to include here, but if the
      // ActiveMQ server disappears while balboa-agent is running, and isn't
      // available when these lines try to execute, the `.close()` call pauses
      // forever (at least longer than my patience to test), and the
      // `.setCloseTimeout(...)` call does not do what the documentation says
      // it will.
      //     amqConnection.setCloseTimeout(1)
      //     amqConnection.close()

      logger info "Stopping the JMX Reporter."
      jmxReporter.stop()
    }
  })

}
