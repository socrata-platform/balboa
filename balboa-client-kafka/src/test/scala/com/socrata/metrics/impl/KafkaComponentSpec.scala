package com.socrata.metrics.impl

import com.socrata.integration.kafka.util.{BalboaMessageClientTestHarness, BalboaClientTestHarness}
import org.scalatest.WordSpec


/**
 * Kafka Conmponents Tests.
 *
 * This test inherits a lot of its design
 */
class KafkaComponentSpec extends BalboaMessageClientTestHarness  {
  override val producerCount: Int = 1
  override val serverCount: Int = 4
  override val consumerCount: Int = 1
  override val topic: String = "component_topic"
}
