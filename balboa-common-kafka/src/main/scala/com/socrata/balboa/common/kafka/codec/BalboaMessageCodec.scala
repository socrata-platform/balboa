package com.socrata.balboa.common.kafka.codec

// scalastyle:off

import com.socrata.balboa.metrics.Message
import com.socrata.balboa.metrics.impl.JsonMessage
import kafka.utils.VerifiableProperties
import org.slf4j.LoggerFactory

/**
 * Reusable Balboa Kafka Message Codec.  This is class can be used by Kafka defined how to serialize and deserialize
 * Metrics messages for Kafka purposes.  This class is used for pointing Kafka to.
 *
 * Note: Required to have [[VerifiableProperties]] argument in constructor.
 * See [[kafka.serializer.Encoder]] and [[kafka.serializer.Decoder]]
 */
class BalboaMessageCodec(properties: VerifiableProperties = null) extends BalboaMessageCodecLike

/**
 * Codec for transforming Metrics Messages.
 */
trait BalboaMessageCodecLike extends KafkaCodec[Message] {

  private val Log = LoggerFactory.getLogger(classOf[BalboaMessageCodecLike])

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
   * @return null if failed to convert bytes to [[Message]]
   */
  override def fromBytes(bytes: Array[Byte]): Message = bytesToJSON(bytes) match {
    case Left(error) =>
      Log.error(s"Failed to convert bytes to message. Error: $error")
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
