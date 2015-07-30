package com.socrata.balboa.producer.kafka

import java.util.Properties

import com.socrata.balboa.common.codec.{BalboaMessageCodec, StringCodec}
import com.socrata.balboa.common.{AddressAndPort, Message}

/**
 * Convenient wrapper for creating a Kafka Producer meant for Balboa purposes.
 *
 * Reference: [[GenericKafkaProducer]].
 */
case class BalboaKafkaProducer(topic: String,
                               brokers: List[AddressAndPort] = List.empty,
                               properties : Option[Properties] = None) extends
GenericKafkaProducer[String, Message, StringCodec, BalboaMessageCodec](topic, brokers, properties)
