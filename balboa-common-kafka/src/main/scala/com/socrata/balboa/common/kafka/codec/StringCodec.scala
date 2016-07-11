package com.socrata.balboa.common.kafka.codec

// scalastyle:off

import kafka.utils.VerifiableProperties

/**
 * Simple Converter that encodes/decodes UTF8 Strings.
 *
 * Note: Required to have [[VerifiableProperties]] argument in constructor.
 * See [[kafka.serializer.Encoder]] and [[kafka.serializer.Decoder]]
 */
class StringCodec(properties: VerifiableProperties = null) extends StringCodecLike

/**
 * Codec that converts Strings to Bytes.
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
