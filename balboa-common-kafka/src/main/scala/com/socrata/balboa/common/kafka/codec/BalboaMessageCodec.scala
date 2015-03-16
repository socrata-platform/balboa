package com.socrata.balboa.common.kafka.codec

import com.socrata.balboa.metrics.Message
import com.socrata.balboa.metrics.impl.JsonMessage
import kafka.utils.VerifiableProperties


/**
 * Reusable Balboa Kafka Message Codec.  This is class can be used by Kafka defined how to serialize and deserialize
 * Metrics messages for Kafka purposes.  This class is used for pointing Kafka to.
 *
 * Note: Required to have [[VerifiableProperties]] argument in constructor.
 * See [[kafka.serializer.Encoder]] and [[kafka.serializer.Decoder]]
 */
class BalboaMessageCodec(properties: VerifiableProperties = null) extends BalboaMessageCodecLike
