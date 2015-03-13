package com.socrata.metrics.impl

import java.io.File
import java.util.Properties

import com.socrata.balboa.metrics.Message
import com.socrata.metrics.components.{EmergencyFileWriterComponent, MessageQueueComponent}
import com.socrata.metrics.producer.{BalboaKafkaProducer, BalboaProducer}
import kafka.common.FailedToSendMessageException
import org.slf4j.LoggerFactory

/**
 * Kafka component that manages sending metric messages to a Kafka Cluster identified
 * by the brokers in [[KafkaInformation]].  This class actually fabricates the queue abstraction for backwards
 *  compatibility purposes.  A call to send to [[com.socrata.metrics.components.MessageQueueComponent.MessageQueueLike.send()]]
 *  actually attempts to send a message synchronously to a Kafka Cluster.
 *
 * <br>
 *   In order to use this component a client must call start() prior to sending in any messages.
 *   If a client does not call start prior to a call to send, the next following call to send will
 *   try to restart the "Queue" and send the message.  If a message fails to send and throws an
 *   exception the message is automatically written to the emergency file.
 */
trait KafkaComponent extends MessageQueueComponent {
  self: KafkaInformation with EmergencyFileWriterComponent =>

  def properties: Properties = new Properties()

  /**
   * Internal Dispatching instance that sends messages via a Kafka Producer.
   */
  class KafkaDispatcher extends MessageQueueLike {

    private val Log = LoggerFactory.getLogger(classOf[KafkaDispatcher])

    /** Need to call [[start()]] to initialize this*/
    var producer: BalboaKafkaProducer[String, Message] = null

    /** File writer for incomplete jobs See [[EmergencyFileWriter()]] */
    val emergencyFileWriter = EmergencyFileWriter(new File(backupFile))

    /** See [[MessageQueueLike.start()]] */
    override def start(): Unit = {
      Log.debug("Starting Kafka Dispatcher")
      producer = BalboaProducer.cons(topic, brokers) match {
        case Left(error) =>
          Log.warn(s"Unable to create producer due to: $error")
          null
        case Right(p) =>
          Log.debug("Kafka Dispatcher successfully started")
          p
      }
    }

    /** See [[MessageQueueLike.stop()]] */
    override def stop(): Unit = {
      // Closing the producer doesn't really allow this
      Log.debug("Shutting down Kafka Dispatcher")
      producer.close()
      producer = null
    }

    /** See [[MessageQueueLike.send()]] */
    override def send(msg: Message): Unit = producer match {
      case p: BalboaKafkaProducer[String, Message] => sendAndHandleError(p, msg)
      case _ =>
        Log.warn(s"Producer not initialized!  Attempting to restart producer...")
        start()
        // If the restart was successful attempt to send message
        producer match {
          case p: BalboaKafkaProducer[String, Message] => sendAndHandleError(p, msg)
          case _ =>
            Log.error(s"Failed to restart producer, writing to emergency file")
            emergencyFileWriter.send(msg)
        }
    }

    /**
     * Attempts to send message utilizing the given producer. If the send fails and an exception is thrown, the message
     * is written to the emergency file.
     *
     * @param p Producer to use to send file.
     * @param msg The Message to send.
     */
    private def sendAndHandleError(p: BalboaKafkaProducer[String, Message], msg: Message) = {
      try {
        p.send(msg)
      } catch {
        case e: FailedToSendMessageException =>
          Log.error(s"Unable to send message $msg.  writing to emergency file", e)
          emergencyFileWriter.send(msg)
      }
    }
  }

  /** See [[MessageQueueComponent.MessageQueue()]] */
  override def MessageQueue(): MessageQueueLike = new KafkaDispatcher

}
