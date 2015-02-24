package com.socrata.balboa.kafka.consumer

import com.socrata.balboa.metrics.impl.JsonMessage
import com.socrata.balboa.metrics.Message
import com.socrata.balboa.metrics.data.DataStore
import kafka.consumer.KafkaStream

/**
 * Consumer that ingest metrics and funnels them into an underlying data store.
 */
case class BalboaConsumer(stream: KafkaStream[Array[Byte],Array[Byte]],
                          waitTime: Long,
                          ds: DataStore) extends PersistentConsumer[Message](stream, waitTime) {

  /**
   * @param m Metrics message to persist to the data store
   */
  override protected def persist(m: Message): Unit = ds.persist(m.getEntityId, m.getTimestamp, m.getMetrics)

  /**
   * Converts a Kafka key, message pair into an instance of type "Message". @see com.socrata.balboa.metrics.Message
   *
   * @return A converted key-message pair or a reason for the failure.
   */
  override protected def convert(key: Array[Byte], message: Array[Byte]): Either[String, Message] = bytesToJSON(message)

  /**
   * @return The String representation of the message in the Right or Error message in the left.
   */
  private def bytesToString(bytes: Array[Byte]): Either[String, String] = bytes match {
    // Currently no compression codecs are used, therefore it is a simple translation
    case a: Array[Byte] => Right(new String(bytes.map(_.toChar)))
    case _ => Left("Null byte array reference cannot be converted to a String")
  }

  /**
   * @return JSON Metric message that was represented by an array of bytes (Right) or Error Message (Left)
   */
  private def bytesToJSON(bytes: Array[Byte]): Either[String, Message] = {
    bytesToString(bytes) match {
      case Right(s) =>
        try
          Right(new JsonMessage(s))
        catch {
          case e: Exception =>
            val m = e.getMessage
            Left(s"Unable to parse JSON Metric Message, Exception message: $m")
        }
      case Left(error) => Left(error)
    }
  }
}
