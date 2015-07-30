package com.socrata.balboa.common.codec

import kafka.serializer.{Decoder, Encoder}

/**
 * Base Specification for Encoding and Decoding anything that is sent via Kafka.  In order to utilize your own KafkaCodec,
 *  you must have a class that meets the correct specification as well as have a constructor with an argument signature of
 *  "properties: VerifiableProperties = null".  This a requirement imposed by Kafka due to the use of reflection to create
 *  appropriate instances.
 *
 * <br>
 * Examples: [[BalboaMessageCodec]], [[BalboaMessageCodecLike]], [[StringCodec]], and [[StringCodecLike]]
 * Reference: [[Decoder]] and [[Encoder]].
 *
 * @tparam A Type to convert to and from Kafka MessageAndMetadata key and message pair
 */
trait BalboaCodec[A] extends Decoder[A] with Encoder[A]
