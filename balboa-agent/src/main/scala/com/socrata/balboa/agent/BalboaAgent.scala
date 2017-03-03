package com.socrata.balboa.agent

import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}
import javax.jms.{ExceptionListener, JMSException}

import com.blist.metrics.impl.queue.MetricJmsQueue
import com.codahale.metrics.JmxReporter
import com.socrata.balboa.agent.metrics.BalboaAgentMetrics
import com.socrata.balboa.util.FileUtils
import com.socrata.metrics.MetricQueue
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.apache.activemq.{ActiveMQConnection, ActiveMQConnectionFactory}

import util.control.NonFatal

/**
  * Balboa Agent class serves as the entry point for running the existing
  * application.
  *
  * This process sets up a scheduler to run at a fixed interval, read metric
  * files from a directory, and send the metrics in the files to an ActiveMQ
  * server.
  */
object BalboaAgent extends App with StrictLogging {
  val conf = new AgentConfig(ConfigFactory.load())
  logger info "Starting balboa-agent."
  private val scheduler = Executors.newScheduledThreadPool(1)
  private val dataDir = conf.dataDirectory
  BalboaAgentMetrics.numFiles("data", dataDir, Some(FileUtils.isBalboaDataFile))
  BalboaAgentMetrics.numFiles("broken", dataDir, Some(FileUtils.isBalboaBrokenFile))
  BalboaAgentMetrics.numFiles("lock", dataDir, Some(FileUtils.isBalboaLockFile))
  BalboaAgentMetrics.numFiles("immutable", dataDir, Some(FileUtils.isBalboaImmutableFile))
  private val jmxReporter = JmxReporter.forRegistry(BalboaAgentMetrics.registry).build()
  logger info "Starting the JMX Reporter."
  jmxReporter.start()

  val metricPublisher = conf.transportType match {
    case Mq => amqMetricQueue()
    case Http =>
      HttpMetricQueue(
        conf.balboaHttpUrl,
        conf.balboaHttpTimeout,
        conf.balboaHttpMaxRetryWait
      )
  }

  val future: ScheduledFuture[_] = scheduler.scheduleWithFixedDelay(new Runnable {
    override def run(): Unit = {
      try {
        val mc = new MetricConsumer(dataDir, metricPublisher)
        try {
          logger debug s"Attempting to run Metric Consumer $mc"
          mc.run() // Recursively reads all metric data files and writes them to a queue.
        } catch {
          case e: Exception => logger.error("Metric consumer failed", e)
        } finally {
          logger debug s"Attempting to close Metric Consumer $mc"
          mc.close() // Release all associated resources.
        }
      } catch {
        case NonFatal(e) =>
          logger.error("Could not create a new MetricConsumer for '{}'.", dataDir, e)
        case e: Throwable =>
          logger.error("Unrecoverable exception occurred while creating a new MetricConsumer for '{}'. Exiting.",
            dataDir, e)
          // If an exception escapes from this method, all future scheduled
          // executions of this Runnable will be stopped, leaving balboa-agent
          // in a broken, but running state as the agent itself will not exit,
          // but neither will it process any metric files. Adding the sys.exit
          // for an unrecoverable exception will make it clear to any
          // monitoring software that there is a problem here.
          sys.exit(1)
      }
    }
  }, conf.initialDelayMs, conf.interval, TimeUnit.MILLISECONDS)

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      logger info "balboa-agent is shutting down."
      logger info "Canceling current and future runs."
      future.cancel(false)
      metricPublisher.close()
      logger info "Stopping the JMX Reporter."
      jmxReporter.stop()
    }
  })


  // scalastyle:off method.length
  def amqMetricQueue(): MetricQueue = {
    logger info "Initializing ActiveMQ connection factory. " +
      "(This is setting the username, password and destination.)"
    val amqConnectionFactory = new ActiveMQConnectionFactory(
      conf.activemqUser, conf.activemqPassword, conf.activemqServer)

    // The ActiveMQ libraries can be obscure when they are misbehaving. The hope
    // is that by registering this listener, additional information will be
    // written to the logs for troubleshooting.
    amqConnectionFactory.setTransportListener(new LoggingTransportListener())

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
        logger error("ActiveMQ connection encountered an error.", exception)
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
      amqConnection.setCloseTimeout(conf.activemqCloseTimeout)
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
    logger info s"Sending metrics to ActiveMQ queue '${conf.activemqQueue}'"
    new MetricJmsQueue(amqConnection, conf.activemqQueue, conf.bufferSize)
  }
}
