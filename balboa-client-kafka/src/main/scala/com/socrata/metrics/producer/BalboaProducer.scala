package com.socrata.metrics.producer

import com.socrata.balboa.metrics.Message
import kafka.producer.Producer

/**
 * Light wrapper around
 */
class BalboaProducer(topic: String, producer: Producer[String, Message]) extends KafkaProducer[Message](topic, producer) {



}
