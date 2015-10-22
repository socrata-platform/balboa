package com.socrata.balboa.service.kafka.test.util

import java.util.Properties

import com.socrata.balboa.common.kafka.codec.{BalboaMessageCodec, StringCodec}
import com.socrata.balboa.common.kafka.util.StagingAndRCEnvironment
import com.socrata.balboa.metrics.Message
import com.socrata.balboa.metrics.util.AddressAndPort
import com.socrata.balboa.service.kafka.consumer.{BalboaConsumerGroup, KafkaConsumerGroupComponent}
import com.socrata.integration.kafka.util.BalboaClientTestHarness
import com.socrata.metrics.producer.{BalboaKafkaProducer, GenericKafkaProducer}
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
  val retries = 3

  // We will create our own consumer group.
  // There for we will not create any default consumers or groups.
  override val consumerCount: Int = 0
  override val consumerGroupCount: Int = 0

  def genConsumerGroup(config: ConsumerConfig): KafkaConsumerGroupComponent[String,Message] = {
    new BalboaConsumerGroup(config, topic, numPartitions, dataStore, waitTime, retries)
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
  override val numPartitions: Int = StagingAndRCEnvironment.NUM_PARTITIONS
  override val replicationFactor: Int = StagingAndRCEnvironment.REPLICATION_FACTOR
  override val serverCount: Int = StagingAndRCEnvironment.SERVER_COUNT
}

// TODO Add Environments for different Production environments.
