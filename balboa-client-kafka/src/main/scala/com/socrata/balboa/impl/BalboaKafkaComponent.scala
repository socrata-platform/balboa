package com.socrata.balboa.impl

// scalastyle:off

import java.io.File
import java.util.Properties

import com.socrata.balboa.metrics.Message
import com.socrata.metrics.components.{EmergencyFileWriterComponent, MessageQueueComponent}
import com.socrata.metrics.producer.BalboaKafkaProducer
import kafka.common.FailedToSendMessageException
import org.slf4j.LoggerFactory

/**
 * Kafka component that manages sending metric messages to a Kafka Cluster identified
 * using the metadata broker list found in [[KafkaProducerInformation]].  This class fabricates the queue abstraction for backwards
 *  compatibility purposes.  A call to send to [[com.socrata.metrics.components.MessageQueueComponent.MessageQueueLike.send()]]
 *  attempts to send a message synchronously to a Kafka Cluster.
 *
 * <br>
 *   In order to use this component a client must call start() prior to sending in any messages.
 *   If a client does not call start prior to a call to send, the next following call to send will
 *   try to restart the "Queue" and send the message.  If a message fails to send and throws an
 *   exception the message is automatically written to the emergency file.
 */
trait BalboaKafkaComponent extends MessageQueueComponent {
  self: KafkaProducerInformation with EmergencyFileWriterComponent =>

  def properties: Properties = new Properties()

  /**
   * Internal Dispatching instance that sends messages via a Kafka Producer.
   */
  class KafkaDispatcher extends MessageQueueLike {

    private val Log = LoggerFactory.getLogger(classOf[KafkaDispatcher])

    /** Need to call [[start()]] to initialize this*/
    var producer: BalboaKafkaProducer = null

    /** File writer for incomplete sent messages. See [[EmergencyFileWriter()]] */
    val emergencyFileWriter: Option[EmergencyFileWriter] = emergencyFile match {
      case f: File =>
        if (!f.exists()) f.createNewFile()
        Some(EmergencyFileWriter(f))
      case _ => None
    }

    /** See [[MessageQueueLike.start()]] */
    override def start(): Unit = {
      Log.debug("Starting Kafka Dispatcher")
      producer = BalboaKafkaProducer(topic, brokers)
    }

    /** See [[MessageQueueLike.stop()]] */
    override def stop(): Unit = {
      // Closing the producer doesn't really allow this
      Log.debug("Shutting down Kafka Dispatcher")
      producer.close()
      producer = null
    }

    /** See [[MessageQueueLike.send()]] */
    override def send(msg: Message): Unit = try {

      /*
      Dev Notes: Kafka did a poor job documenting which error is thrown in the case of a send failure.  We had to refer
      to the source code to identify that most errors are wrapped FailedToSendMessageException.  This is not a reliable
      assumption and prone to the introduction of new Checked Exceptions.
       */
      producer match {
        case p: BalboaKafkaProducer => // Producer already ready.
        case _ => start()
      }

      producer.send(msg)
    } catch {
      case e: FailedToSendMessageException =>
        val errMsg = s"Unable to send message $msg.  writing to emergency file: ${emergencyFile.getAbsolutePath}"
        // Always log that an error occurred
        Log.error(errMsg, e)
        emergencyFileWriter match {
          case Some(efw) => efw.send(msg)
          case None => Log.error(errMsg)
        }
    }
  }

  /** See [[MessageQueueComponent.MessageQueue()]] */
  override def MessageQueue(): MessageQueueLike = new KafkaDispatcher

}
