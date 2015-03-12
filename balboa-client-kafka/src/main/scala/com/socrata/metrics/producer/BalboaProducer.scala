package com.socrata.metrics.producer

import com.socrata.balboa.kafka.codec.{BalboaMessageCodec, StringCodec}
import com.socrata.balboa.kafka.util.AddressAndPort
import com.socrata.balboa.metrics.Message

/**
 * Convenient wrapper for creating a Kafka Producer meant for Balboa purposes.
 */
object BalboaProducer {

  def cons(topic: String,
           brokers: List[AddressAndPort] = List.empty): Either[String, BalboaKafkaProducer[String, Message]] =
    BalboaKafkaProducer.cons[String, Message, StringCodec, BalboaMessageCodec](topic, brokers)

}