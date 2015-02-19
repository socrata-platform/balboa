package com.socrata.balboa.kafka.consumer

import java.io.IOException

import com.socrata.balboa.metrics.data.DataStore
import com.socrata.balboa.metrics.impl.JsonMessage
import kafka.consumer.{ConsumerIterator, KafkaStream}

/**
 * Created by Michael Hotan on 2/18/15.
 *
 * Balboa Kafka Consumer
 */
sealed abstract class BalboaConsumer(stream: KafkaStream[Array[Byte], Array[Byte]]) extends Runnable {

  override def run(): Unit = {
    val it: ConsumerIterator[Array[Byte], Array[Byte]] = stream.iterator()
    while (it.hasNext())
      onMessage(it.next().message())
  }

  /**
   * Notification of message receipt.
   *
   * @param message Received Kafka Message
   */
  def onMessage(message: Array[Byte]): Unit

  /**
   * Converts an array of bytes to a String where each character is the
   *
   * @param bytes The bytes to convert.
   * @return The String representation of the Byte Array.
   */
  def bytesToString(bytes: Array[Byte]): String = {
    new String(bytes.map(_.toChar))
  }

}
case class TestConsumer(stream: KafkaStream[Array[Byte], Array[Byte]]) extends BalboaConsumer(stream) {

  /**
   * Notification of message receipt.  Simply prints the message to the console.
   *
   * @param message Recieved Kafka Message
   */
  override def onMessage(message: Array[Byte]): Unit = {
    val m: String = bytesToString(message)
    val id: Long = Thread.currentThread().getId
    Console.println(s"Thread $id receiving this message: $m")
  }

}
case class DSConsumer(stream: KafkaStream[Array[Byte], Array[Byte]], ds: DataStore) extends BalboaConsumer(stream) {
  /**
   * Notification of message receipt.  Saves each individual message to the data store.
   *
   * @param message Received Kafka Message
   */
  override def onMessage(message: Array[Byte]): Unit = {
    // I feel this a lot of work mapping bytes to char, creating a string object
    // convert it to JSON.
    val m: JsonMessage = new JsonMessage(bytesToString(message))

    try {
      ds.persist(m.getEntityId, m.getTimestamp, m.getMetrics)
    } catch {
      // TODO How should we failed writes to DS.
      case e: IOException => ???
    }
  }

}

