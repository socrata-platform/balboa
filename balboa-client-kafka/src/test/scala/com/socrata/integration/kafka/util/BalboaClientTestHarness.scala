package com.socrata.integration.kafka.util

import java.util.Properties

import com.socrata.balboa.kafka.codec.{BalboaMessageCodec, StringCodec, KafkaCodec}
import com.socrata.balboa.kafka.util.AddressAndPort
import com.socrata.balboa.metrics.Message
import com.socrata.metrics.producer.BalboaKafkaProducer
import kafka.consumer.{ConsumerConfig, Consumer, ConsumerConnector}
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{Utils, TestUtils}
import org.scalatest.BeforeAndAfterEach

import scala.collection.mutable

/**
 * ScalaTest Harness base for testing Kafka Client.  Currently this test harness is configured such that
 * a number of producers all publish to the same Kafka topic.  Then a group of consumers all with different group
 * ids are created.
 *
 * <br>
 * Setup is conducted with [[BeforeAndAfterEach.beforeEach()]].  Within each test case setup process a client defined
 * set Kafka Servers, Producers, and Consumers are created.  An [[kafka.zk.EmbeddedZookeeper]] instance is also
 * created and started but defaults to one server.
 *
 * TearDown is conducted within [[BeforeAndAfterEach.afterEach()]].  Within each test case teardown process Kafka servers,
 * producers, and consumers are all stopped.
 */
trait BalboaClientTestHarness[K, M] extends KafkaServerTestHarness {

  val producerCount: Int
  val consumerCount: Int
  val serverCount: Int
  val topic: String

  /**
   * Topic Replication factor
   */
  val replicationFactor: Int = 1

  /**
   * Number of partitions for a given topic
   */
  val numPartitions: Int = 1

  /**
   * Create a new producer with given parameters.  Currently a work around until we get how to pass manifest
   * around at runtime.
   *
   * @param topic Topic to create for producer
   * @param brokers List of kafka brokers
   * @param properties default properties to use
   * @return Producer
   */
  protected def genProducer(topic: String,
                            brokers: List[AddressAndPort] = List.empty,
                            properties: Option[Properties] = None): BalboaKafkaProducer[K,M]

  /**
   * This number is assigned by the current number of Kafka servers inside our cluster.
   */
  val DEFAULT_SERVER_COUNT = 4
  lazy val producerConfig = new Properties
  lazy val consumerConfig = new Properties
  lazy val serverConfig = new Properties
  override lazy val configs = {
    val cfgs = TestUtils.createBrokerConfigs(serverCount)
    cfgs.map(_.putAll(serverConfig))
    cfgs.map(new KafkaConfig(_))
  }

  /**
   * A buffer of different Consumer Connector that all point to different
   */
  var consumers = mutable.Buffer[ConsumerConnector]()
  var producers = mutable.Buffer[BalboaKafkaProducer[K, M]]()

  /**
   * @return URL for bootstrapped Kafka Servers.
   */
  def bootstrapUrl = configs.map(c => c.hostName + ":" + c.port).mkString(",")

  override def setUp(): Unit = {
    super.setUp()
    for(i <- 0 until producerCount)
      producers += genProducer(topic, AddressAndPort.parse(this.bootstrapUrl), Some(producerConfig))


    for(i <- 0 until consumerCount) {
      val indConsumerConfig = new Properties(consumerConfig)
      indConsumerConfig.setProperty("zookeeper.connect", this.zkConnect)
      indConsumerConfig.setProperty("group.id", i.toString)
      consumers += Consumer.create(new ConsumerConfig(indConsumerConfig))
    }

    TestUtils.createTopic(zkClient, this.topic, this.numPartitions, this.replicationFactor, servers)
  }

  override def tearDown(): Unit = {
    producers.map(_.close())
    consumers.map(_.shutdown())
    super.tearDown()
  }

}

trait StringClientTestHarness extends BalboaClientTestHarness[String, String] {

  /**
   * See [[BalboaClientTestHarness.genProducer()]].
   */
  override def genProducer(topic: String,
                           brokers: List[AddressAndPort],
                           properties: Option[Properties]): BalboaKafkaProducer[String, String] =
    BalboaKafkaProducer.cons[String, String, StringCodec, StringCodec](
      topic, AddressAndPort.parse(this.bootstrapUrl), Some(producerConfig)) match {
      case Left(error) => throw new IllegalStateException("Unable to initialize producers: " + error)
      case Right(p) => p
    }
}

trait BalboaMessageClientTestHarness extends BalboaClientTestHarness[String, Message] {

  /**
   * See [[BalboaClientTestHarness.genProducer()]]
   */
  override protected def genProducer(topic: String,
                                     brokers: List[AddressAndPort],
                                     properties: Option[Properties]): BalboaKafkaProducer[String, Message] =
    BalboaKafkaProducer.cons[String, Message, StringCodec, BalboaMessageCodec](
      topic, AddressAndPort.parse(this.bootstrapUrl), Some(producerConfig)) match {
      case Left(error) => throw new IllegalStateException("Unable to initialize producers: " + error)
      case Right(p) => p
    }
}
