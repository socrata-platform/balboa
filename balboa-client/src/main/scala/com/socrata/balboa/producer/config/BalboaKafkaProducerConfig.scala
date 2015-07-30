package com.socrata.balboa.producer.config

import com.socrata.balboa.common.AddressAndPort
import com.socrata.balboa.common.config.{Configuration, Keys}

/**
 * Kafka Client Configuration Object
 */
object BalboaKafkaProducerConfig extends BalboaProducerConfig {

  /**
   * @return A list of Address and Ports that reference the Kafka Brokers to pull metadata from.
   */
  def brokers: Seq[AddressAndPort] = AddressAndPort.parse(Configuration.get().getString(Keys.KAFKA_METADATA_BROKERLIST))

  /**
   * @return The Kafka topic to publish data to.
   */
  def topic: String = Configuration.get().getString(Keys.KAFKA_TOPIC)

}
