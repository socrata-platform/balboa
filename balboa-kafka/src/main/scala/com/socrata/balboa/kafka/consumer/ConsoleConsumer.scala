package com.socrata.balboa.kafka.consumer

import com.socrata.balboa.metrics.impl.JsonMessage
import kafka.consumer.KafkaStream

/**
 * Example Kafka Consumer.
 *
 * Simply assumes all
 */
class ConsoleConsumer(stream: KafkaStream[Array[Byte],Array[Byte]]) extends KafkaConsumer[String](stream, 0) {

  /**
   * Converts a Kafka key, message pair into an instance of type "String".
   * <b>
   * For a reference to Kafka Messages see: {@link http://kafka.apache.org/documentation.html#messages}
   *
   * @param key Kafka message key
   * @param message Kafka message content
   * @return A converted key-message pair or a reason for the failure.
   */
  override protected def convert(key: Array[Byte], message: Array[Byte]): Either[String, String] = (key, message)
  match {
    case (k: Array[Byte], m: Array[Byte]) => Right(s"key: $k message: $m")
    case (k: Array[Byte], _) => Right(s"key: $k")
    case (_, m: Array[Byte]) => Right(s"message: $m")
    case _ => Left("Null key and message")
  }

  /**
   * Function that handles the reception of new Messages
   */
  override protected def onMessage(message: String): Unit = {
    val id: Long = Thread.currentThread().getId
    val s: String = message.toString
    Console.println(s"Thread $id receiving this message: $s")
  }

  /**
   * Printing messages to the console. Always ready.
   */
  override protected def ready: Boolean = true

}
