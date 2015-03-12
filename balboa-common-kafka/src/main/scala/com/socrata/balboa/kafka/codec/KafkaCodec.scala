package com.socrata.balboa.kafka.codec

import com.socrata.balboa.metrics.Message
import com.socrata.balboa.metrics.impl.JsonMessage
import kafka.serializer.{Decoder, Encoder}

/**
 * Base Specification for Encoding and Decoding anything that is sent via Kafka
 *
 * Reference: [[Decoder]] and [[Encoder]]
 *
 * @tparam A Type to convert to and from Kafka MessageAndMetadata key and message pair
 */
trait KafkaCodec[A] extends Decoder[A] with Encoder[A]

/**
 * Codec for transforming Metrics Messages.
 */
trait BalboaMessageCodecLike extends KafkaCodec[Message] {

  /**
   * Preconditions: message entity id, timestamp, and metrics should not be null.
   *
   * See: [[kafka.serializer.Encoder]]
   */
  override def toBytes(m: Message): Array[Byte] = {
    val jm = new JsonMessage()
    jm.setEntityId(m.getEntityId)
    jm.setTimestamp(m.getTimestamp)
    jm.setMetrics(m.getMetrics)
    m.serialize()
  }

  /**
   * Postconditions: null on failure to serialize or deserialize.
   *
   * See: [[kafka.serializer.Decoder]]
   */
  override def fromBytes(bytes: Array[Byte]): Message = bytesToJSON(bytes) match {
    case Left(error) =>
      //      TODO Log error
      null
    case Right(message) => message
  }

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
            Left(s"Unable to parse JSON Metric Message, Exception: $m")
        }
      case Left(error) => Left(error)
    }
  }

}

/**
 *
 */
trait StringCodecLike extends KafkaCodec[String] {

  /**
   * Converts String to UTF8 encoded byte array
   *
   * @return null if string is null, other UTF8 Byte encoded array
   */
  override def toBytes(string: String): Array[Byte] = string match {
    case s: String => s.toCharArray.map(c => c.toByte)
    case _ => null
  }

  /**
   * Converts UTF8 bytes to String.  If bytes is null then null is returned.
   *
   * @return null if bytes is null, otherwise the converted String.
   */
  override def fromBytes(bytes: Array[Byte]): String = bytes match {
    case b: Array[Byte] => new String(bytes, "UTF8")
    case _ => null
  }

}