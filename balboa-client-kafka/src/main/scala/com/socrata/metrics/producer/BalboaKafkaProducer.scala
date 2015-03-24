package com.socrata.metrics.producer

import java.util.Properties

import com.socrata.balboa.common.kafka.codec.{BalboaMessageCodec, StringCodec}
import com.socrata.balboa.common.kafka.util.AddressAndPort
import com.socrata.balboa.metrics.Message

/**
 * Convenient wrapper for creating a Kafka Producer meant for Balboa purposes.
 *
 * Reference: [[GenericKafkaProducer]].
 */
case class BalboaKafkaProducer(topic: String,
                               brokers: List[AddressAndPort] = List.empty,
                               properties : Option[Properties] = None) extends
GenericKafkaProducer[String, Message, StringCodec, BalboaMessageCodec](topic, brokers, properties)
