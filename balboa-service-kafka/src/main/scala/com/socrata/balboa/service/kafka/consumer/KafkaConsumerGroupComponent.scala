package com.socrata.balboa.service.kafka.consumer

// scalastyle:off

import com.socrata.balboa.service.consumer.ConsumerGroupComponent

/**
 * A group of Kafka Consumers that handle kafka message ingestion, translation, and potentially storage.  Consumer groups
 * are required to handle ingesting messages in parallel and effectively maximizing throughput.
 *
 * Usage Notes:
 * Like [[KafkaConsumerComponent]] this is meant to be extensible and composed of Kafka Consumers.  The best way to use
 * this trait is to extend it your self with a KafkaConsumerComponent you have already defined.
 *
 * @tparam K Kafka Key type.
 * @tparam M Kafka Message type.
 */
trait KafkaConsumerGroupComponent[K,M] extends ConsumerGroupComponent[(K,M)] {
  self: KafkaConsumerComponent[K,M] =>

  /**
   * @return A list of Kafka Consumers that belong exclusively to this group.
   */
  override val consumers: List[KafkaConsumer]

}
