package com.socrata.balboa.common.kafka.codec

import kafka.utils.VerifiableProperties

/**
 * Simple Converter that encodes/decodes UTF8 Strings.
 *
 * Note: Required to have [[VerifiableProperties]] argument in constructor.
 * See [[kafka.serializer.Encoder]] and [[kafka.serializer.Decoder]]
 */
class StringCodec(properties: VerifiableProperties = null) extends StringCodecLike
