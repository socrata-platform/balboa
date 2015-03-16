package com.socrata.balboa.common.kafka.consumer

import com.socrata.balboa.service.consumer.ConsumerGroupComponent

/**
 * A group of Kafka Consumers that handle kafka message ingestion, translation, and potentially storage.  Consumer groups
 * are required to handle ingesting messages parallel and effectively maximizing throughput.
 *
 * @tparam K Kafka Key type.
 * @tparam M Kafka Message type.
 */
trait KafkaConsumerGroup[K,M] extends ConsumerGroupComponent[(K,M)] {
  self: KafkaConsumerComponent[K,M] =>

  /**
   * @return A list of Kafka Consumers that belong exclusively to this group.
   */
  override def consumers: List[KafkaConsumer]

}