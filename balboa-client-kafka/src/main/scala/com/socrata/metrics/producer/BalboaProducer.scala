package com.socrata.metrics.producer

import java.util.Properties

import com.socrata.balboa.common.kafka.codec.{BalboaMessageCodec, StringCodec}
import com.socrata.balboa.common.kafka.util.AddressAndPort
import com.socrata.balboa.metrics.Message

/**
 * Convenient wrapper for creating a Kafka Producer meant for Balboa purposes.
 */
object BalboaProducer {

  /**
   * @return A [[BalboaKafkaProducer]] that sends keys as [[String]] and kafka messages as Balboa [[Message]]s
   */
  def cons(topic: String,
           brokers: List[AddressAndPort] = List.empty,
           properties : Option[Properties] = None): Either[String, BalboaKafkaProducer[String, Message]] =
    BalboaKafkaProducer.cons[String, Message, StringCodec, BalboaMessageCodec](topic, brokers, properties)

}