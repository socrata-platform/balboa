package com.socrata.integration.kafka.util

import java.util.Properties

import com.socrata.balboa.metrics.Message
import com.socrata.balboa.common.kafka.codec.{KafkaCodec, BalboaMessageCodec, StringCodec}
import com.socrata.balboa.common.kafka.util.AddressAndPort
import com.socrata.metrics.producer.{BalboaKafkaProducer, GenericKafkaProducer}
import kafka.consumer._
import kafka.integration.KafkaServerTestHarness
import kafka.serializer.Encoder
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.ProducerConfig
import org.scalatest.BeforeAndAfterEach

import scala.collection.{Map, mutable}

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
trait BalboaClientTestHarness[K,M,KE <: KafkaCodec[K],ME <: KafkaCodec[M]]
  extends KafkaServerTestHarness {

  val producerCount: Int
  val consumerCount: Int
  val serverCount: Int
  val topic: String

  val CONSUMER_TIMEOUT_MS = 120000

  /**
   * Default to One group with numerous consumers.
   */
  val consumerGroupCount: Int = 1

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
                            properties: Option[Properties] = None): GenericKafkaProducer[K,M,KE,ME]

  /**
   * This number is assigned by the current number of Kafka servers inside our cluster.
   */
  val DEFAULT_SERVER_COUNT = 4
  lazy val producerConfig = new Properties
  lazy val consumerConfig = new Properties
  lazy val serverConfig = new Properties
  override lazy val configs = {
    val cfgs = TestUtils.createBrokerConfigs(serverCount)
    cfgs.foreach(_.putAll(serverConfig))
    cfgs.map(new KafkaConfig(_))
  }

  /**
   * A buffer of different Consumer Connector that all point to different
   */
  var consumers = mutable.Buffer[ConsumerConnector]()
  var producers = mutable.Buffer[GenericKafkaProducer[K,M,KE,ME]]()

  /**
   * Mapping from Consumer group to all the consumers that belong in that group.
   */
  var consumerMap = Map[Int, List[ConsumerConnector]]()

  /**
   * @return URL for bootstrapped Kafka Servers.
   */
  def bootstrapUrl = configs.map(c => c.hostName + ":" + c.port).mkString(",")

  override def setUp(): Unit = {
    super.setUp()

    Range(0, producerCount).foreach { i =>
      producers += genProducer(topic, AddressAndPort.parse(this.bootstrapUrl), Some(producerConfig))
    }

    Range(0, consumerGroupCount).foreach { cg =>
      Range(0, consumerCount).foreach { c =>
        val config = new ConsumerConfig(
          TestUtils.createConsumerProperties(zkConnect, cg.toString, c.toString)) {
          override val consumerTimeoutMs = CONSUMER_TIMEOUT_MS
        }
        val connector: ConsumerConnector = Consumer.create(config)
        consumers += connector
        consumerMap = consumerMap + ((cg, {consumerMap.getOrElse(cg, Nil) :+ connector}))
      }
    }

    TestUtils.createTopic(zkClient, this.topic, this.numPartitions, this.replicationFactor, servers)
  }

  override def tearDown(): Unit = {
    producers.map(_.close())
    consumers.map(_.shutdown())
    super.tearDown()
  }

  /**
   * For testing purposes producers needs to be configured with certain properties.  Over loads the current state
   * of [[serverConfig]] before being used to create a producer.
   *
   * @return Producer properties for testing purposes
   */
  protected def overloadProducerConfigs(acks: Int = -1, // All messages are in sync.
                                        metadataFetchTimeout: Long = 3000L,
                                        blockOnBufferFull: Boolean = true,
                                        bufferSize: Long = 1024L * 1024L,
                                        retries: Int = 0,
                                        lingerMs: Long = 0): Properties = {
    val p = new Properties(serverConfig)
    p.put(ProducerConfig.ACKS_CONFIG, acks.toString)
    p.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, metadataFetchTimeout.toString)
    p.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, blockOnBufferFull.toString)
    p.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferSize.toString)
    p.put(ProducerConfig.RETRIES_CONFIG, retries.toString)
    p.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100")
    p.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, "200")
    p.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs.toString)
    p
  }
}

case class StringKafkaProducer(topic: String,
                               brokers: List[AddressAndPort],
                               properties: Option[Properties])
  extends GenericKafkaProducer[String, String, StringCodec, StringCodec](topic, brokers, properties)

trait StringClientTestHarness extends BalboaClientTestHarness[String, String, StringCodec, StringCodec] {

  /**
   * See [[BalboaClientTestHarness.genProducer()]].
   */
  override def genProducer(topic: String,
                           brokers: List[AddressAndPort],
                           properties: Option[Properties]): GenericKafkaProducer[String, String, StringCodec, StringCodec] =
    StringKafkaProducer(topic, brokers, properties)
}

trait BalboaMessageClientTestHarness extends BalboaClientTestHarness[String, Message, StringCodec, BalboaMessageCodec] {

  /**
   * See [[BalboaClientTestHarness.genProducer()]]
   */
  override protected def genProducer(topic: String,
                                     brokers: List[AddressAndPort],
                                     properties: Option[Properties]):
  GenericKafkaProducer[String, Message, StringCodec, BalboaMessageCodec] = BalboaKafkaProducer(topic, brokers, properties)

}
