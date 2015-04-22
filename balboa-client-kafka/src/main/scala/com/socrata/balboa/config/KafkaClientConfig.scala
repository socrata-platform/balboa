package com.socrata.balboa.config

import com.socrata.balboa.metrics.config.{Configuration, Keys}
import com.socrata.balboa.metrics.util.AddressAndPort

/**
 * Kafka Client Configuration Object
 */
object KafkaClientConfig extends CoreClientConfig {

  /**
   * @return A list of Address and Ports that reference the Kafka Brokers to pull metadata from.
   */
  def brokers: Seq[AddressAndPort] = AddressAndPort.parse(Configuration.get().getString(Keys.KAFKA_METADATA_BROKERLIST))

  /**
   * @return The Kafka topic to publish data to.
   */
  def topic: String = Configuration.get().getString(Keys.KAFKA_TOPIC)

}
