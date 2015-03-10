package com.socrata.metrics.impl

import com.socrata.balboa.metrics.Message
import com.socrata.metrics.components.{EmergencyFileWriterComponent, MessageQueueComponent}
import org.apache.commons.logging.LogFactory

trait KafkaConfiguration

/**
 * Kafka component that manages sending metric messages to
 */
trait KafkaComponent extends MessageQueueComponent {
   self: KafkaInformation with EmergencyFileWriterComponent =>

  val producer = KafkaProducer

  /**
   * Kafka Queue impersonator for backward compatibility reasons.
   */
  class KafkaQueueImpersonator extends MessageQueueLike {

    // Start should create a Kafka Producer
    override def start(): Unit = {
      // NOOP
    }

    override def stop(): Unit =

    override def send(msg: Message): Unit = ???
  }

  override def MessageQueue(): MessageQueueLike = new KafkaQueueImpersonator

}
