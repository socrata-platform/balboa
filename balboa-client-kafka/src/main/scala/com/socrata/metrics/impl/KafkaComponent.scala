package com.socrata.metrics.impl

import com.socrata.balboa.metrics.Message
import com.socrata.metrics.components.{EmergencyFileWriterComponent, MessageQueueComponent}
import com.socrata.metrics.producer.{BalboaKafkaProducer, BalboaProducer}
import org.slf4j.LoggerFactory

/**
 * Kafka component that manages sending metric messages to a Kafka Cluster identified
 * by the brokers in [[KafkaInformation]].  This service requires
 */
trait KafkaComponent extends MessageQueueComponent {
  self: KafkaInformation with EmergencyFileWriterComponent =>

  private val Log = LoggerFactory.getLogger(classOf[KafkaComponent])

  /**
   * Internal Dispatching instance that sends messages via a Kafka Producer.
   */
  class KafkaDispatcher extends MessageQueueLike {

    // Force clients
    var producer: BalboaKafkaProducer[String, Message] = null

    /**
     * Prepares a KafkaProducer to send messages to the Kafka Cluster.
     */
    override def start(): Unit = {
      Log.debug("Starting ")
      producer = BalboaProducer.cons(topic, brokers) match {
        case Left(error) =>
          Log.warn("Unable to create producer due to: %s".format(error))
          null
        case Right(p) => p
      }
    }

    /**
     * Stop
     */
    override def stop(): Unit = {
      // Closing the producer doesn't really allow this
      Log.debug("Shutting down Kafka Producer")
      producer.close()
      producer = null
    }

    override def send(msg: Message): Unit = producer match {
      case p: BalboaKafkaProducer[String, Message] => p.send(msg)
      case _ =>
        Log.warn(s"Unable to send message $msg. Did you call MessageQueueComponent.start()?")

    }
  }

  override def MessageQueue(): MessageQueueLike = new KafkaDispatcher

}
