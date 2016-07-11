package com.socrata.balboa.impl

import com.socrata.balboa.metrics.Message
import com.socrata.metrics.components.{EmergencyFileWriterComponent, MessageQueueComponent}
import org.slf4j.LoggerFactory

/**
 * Component that contains a collection of other [[MessageQueueComponent]]s.  When a metrics
 * message is sent through the dispatcher the metrics message is dispatched to each one of the other components.
 * This dispatcher does not preform any Component Validation.  If an internal component fails then it writes to its
 * internal emergence handling procedure.
 */
trait BalboaDispatcherComponent extends MessageQueueComponent {
  self: DispatcherInformation with EmergencyFileWriterComponent =>

  class MessageDispatcher extends MessageQueueLike {

    private val Log = LoggerFactory.getLogger(classOf[MessageDispatcher])

    /**
     * Create all the internal queues at queue creation time.
     */
    val queues = components.map(_.MessageQueue())

    /**
     * Initialize the message queue and prepares to recieve messages.
     */
    override def start(): Unit = queues.foreach(_.start())

    /**
     * Stops and destroys the underlying queue.
     */
    override def stop(): Unit = queues.foreach(_.stop())

    /**
     * Sends a message using the underlying queue.
     * @param msg Messsage to send.  Should not be null.
     */
    override def send(msg: Message): Unit = queues.foreach(_.send(msg))
  }

  // scalastyle:off method.name
  override def MessageQueue(): MessageQueueLike = new MessageDispatcher

}
