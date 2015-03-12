package com.socrata.balboa.kafka.consumer

import com.socrata.balboa.kafka.codec.{StringCodecLike, StringCodec}
import kafka.consumer.KafkaStream

/**
 * Example Kafka Consumer.
 * <br>
 *  Treats every Kafka Message as a String that was converted to an array of bytes.
 */
class ConsoleConsumer(stream: KafkaStream[Array[Byte],Array[Byte]])
  extends KafkaConsumer[String](stream, 0) with StringCodecLike {

  /**
   * Function that handles the reception of new Messages
   */
  override protected def onMessage(key: Option[String] = None, message: String): Unit = {
    val id: Long = Thread.currentThread().getId
    val s: String = message.toString
    Console.println(s"Thread $id receiving this message: $s")
  }

  /**
   * Printing messages to the console. Always ready.
   */
  override protected def ready: Boolean = true

}
