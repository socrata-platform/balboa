package com.socrata.balboa.service.kafka.test.util

import java.util.Properties

import com.socrata.balboa.common.codec.{BalboaMessageCodec, StringCodec}
import com.socrata.balboa.common.kafka.TestEnvironment
import com.socrata.balboa.common.{AddressAndPort, Message}
import com.socrata.balboa.producer.kafka.{BalboaKafkaProducer, GenericKafkaProducer}
import com.socrata.balboa.service.kafka.consumer.{BalboaConsumerGroup, KafkaConsumerGroupComponent}
import com.socrata.integration.kafka.util.BalboaClientTestHarness
import kafka.consumer.ConsumerConfig

/**
 * ScalaTest Harness base for testing Kafka Consumption Services.  This harness uses
 */
trait BalboaServiceTestHarness extends BalboaClientTestHarness[String,Message,StringCodec,BalboaMessageCodec] {

  val producerCount: Int
  val serverCount: Int
  val topic: String = "balboa_consumer_group_topic"

  // TODO Need to add embedded Cassandra instance to test the actual consumption.
  val dataStore: MockDataStore = new MockDataStore
  val waitTime = 30L

  // We will create our own consumer group.
  // There for we will not create any default consumers or groups.
  override val consumerCount: Int = 0
  override val consumerGroupCount: Int = 0

  def genConsumerGroup(config: ConsumerConfig): KafkaConsumerGroupComponent[String,Message] = {
    new BalboaConsumerGroup(config, topic, numPartitions, dataStore, waitTime)
  }

  /**
   * See [[BalboaClientTestHarness.genProducer()]].
   */
  override protected def genProducer(topic: String,
                                     brokers: List[AddressAndPort],
                                     properties: Option[Properties]):
  GenericKafkaProducer[String,Message,StringCodec,BalboaMessageCodec] = BalboaKafkaProducer(topic,brokers,properties)
}

trait StagingAndRCServiceTestHarness extends BalboaServiceTestHarness {
  override val numPartitions: Int = TestEnvironment.NUM_PARTITIONS
  override val replicationFactor: Int = TestEnvironment.REPLICATION_FACTOR
  override val serverCount: Int = TestEnvironment.SERVER_COUNT
}

// TODO Add Environments for different Production environments.
