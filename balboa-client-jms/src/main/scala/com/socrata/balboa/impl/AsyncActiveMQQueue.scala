package com.socrata.balboa.impl

import java.io.Closeable

import com.blist.metrics.impl.queue.MetricJmsQueue
import com.socrata.balboa.metrics.Metric
import com.socrata.metrics.{AbstractMetricQueue, IdParts}
import org.apache.activemq.transport.DefaultTransportListener
import org.apache.activemq.{ActiveMQConnection, ActiveMQConnectionFactory}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * This MetricQueue provides non-blocking functionality. It requires use of ActiveMQ for JMS.
 * It should never block, regardless of whether ActiveMQ is reachable.
 *
 * -WARNING- In the event of being disconnected from ActiveMQ, this queue will simply drop metrics!
 *
 * @param connectionUri Connection URI for ActiveMQ. Use of failover transport is highly recommended
 * @param queueName JMS queue name
 */
class AsyncActiveMQQueue(connectionUri: String, queueName: String, bufferSize: Int) extends AbstractMetricQueue {
  private val log = LoggerFactory.getLogger(classOf[AsyncActiveMQQueue])
  private val activeMQFactory: ActiveMQConnectionFactory = {
    val factory = new ActiveMQConnectionFactory(connectionUri)
    factory.setUseAsyncSend(true)
    factory
  }

  private var connection: Option[ActiveMQConnection] = None
  private var underlying: Option[MetricJmsQueue] = None
  private var started = false

  /**
   * Initializes necessary connections with external services. Must be called before create(_). Should not be called
   * again without first calling close().
   */
  def start(): Unit = {
    if (!started) {
      started = true
      Future { activeMQFactory.createConnection() } onComplete {
        case Success(conn) =>
          connection = Some({
            val activeMQConn = conn.asInstanceOf[ActiveMQConnection]
            activeMQConn.addTransportListener(BalboaTransportListener)
            activeMQConn
          })
          underlying = connection.map(new MetricJmsQueue(_, queueName, bufferSize))
        case Failure(e) =>
          // Using failover transport, this should never happen; rather, createConnection() will hang
          // until it finds a connection
          log.error("ActiveMQ initial connection failed. Metrics will not function until a restart!", e)
      }
    } else {
      log.warn("Cannot start ActiveMQQueue... it is already started")
    }
  }

  /**
   * Release current resources associated with this AsyncActiveMQQueue.
   */
  def close(): Unit = {
    val connected = BalboaTransportListener.isConnected
    connection.map(_.removeTransportListener(BalboaTransportListener))
    BalboaTransportListener.close()
    try {
      if (connected) {
        try { underlying.map(_.close()) }
        finally { connection.map(_.close()) }
      } else {
        // ActiveMQ connection can be deadlocked in this situation, so close() can simply hang.. grr
        throw new RuntimeException(
          "Unable to properly shutdown ActiveMQ connections.. resources might not have been properly unallocated!")
      }
    } finally {
      underlying = None
      connection = None
      started = false
    }
  }

  /**
   * Indicates whether this queue is currently connected to ActiveMQ
 *
   * @return True if connected; false otherwise
   */
  def isConnected: Boolean = {
    BalboaTransportListener.isConnected
  }

  /**
   * Add a Metric to Balboa.
 *
   * @param entity Entity which this Metric belongs to (ex: a domain)
   * @param name Metric to store
   * @param value Numeric value of this metric
   * @param timestamp Time when this metric was created
   * @param metricType Type of metric to add
   */
  override def create(entity: IdParts,
                      name: IdParts,
                      value: Long,
                      timestamp: Long,
                      metricType: Metric.RecordType): Unit = {
    underlying match {
      case Some(queue) if BalboaTransportListener.isConnected =>
        queue.create(entity, name, value, timestamp, metricType)
      case Some(_) | None =>
        log.warn(s"Not connected to ActiveMQ. Is it up? Dropping metric - entity: '$entity', metricID: '$name'")
    }
  }

  /**
   * An ActiveMQ TransportListener that simply tracks the status of a connection.
   */
  private object BalboaTransportListener extends DefaultTransportListener with Closeable {
    private var jmsConnected = false

    override def transportInterupted(): Unit = {
      jmsConnected = false
      log.error("Connection to ActiveMQ lost. Metrics will start dropping")
    }

    override def transportResumed(): Unit = {
      jmsConnected = true
      log.info("Connection to ActiveMQ established.")
    }

    def isConnected: Boolean = jmsConnected

    def close(): Unit = {
      jmsConnected = false
    }
  }
}
