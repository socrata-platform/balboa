package com.socrata.balboa.service.kafka

import java.util.Properties

import com.socrata.balboa.metrics.Message
import com.socrata.balboa.metrics.data.{DataStore, DataStoreFactory}
import com.socrata.balboa.service.kafka.consumer.{BalboaConsumerGroup, KafkaConsumerGroupComponent}
import com.typesafe.scalalogging.slf4j.Logger
import joptsimple.OptionSet
import kafka.consumer.{Consumer, ConsumerConfig}
import org.slf4j.LoggerFactory

/**
 * Balboa Kafka Main Application:
 *
 * Entry point for Balboa Kafka Consumer.  This application starts a service that listens
 * on a configurable list of ZooKeeper Host Ports.  This service currently listens for all published Tenant Metrics.
 */
object BalboaKafkaConsumerCLI extends KafkaConsumerCLIBase[String, Message] {

  private val Log = Logger(LoggerFactory getLogger this.getClass)

  private val TOPIC_PERSISTENT_CONSUMER_WAITTIME_KEY = "balboa.kafka.consumer.persistent.waittime"
  private val CASSANDRA_SERVERS_KEY = "cassandra.servers"
  private val CASSANDRA_RETRIES_KEY = "cassandra.retries"
  private val LOCAL_CASSANDRA = "localhost:6062"
  // Default the time we wait for a data store to
  val DEFAULT_PERSISTENT_CONSUMER_WAITTIME = 1000

  // Retrieve the default data store.
  lazy val ds: DataStore = DataStoreFactory.get

  /**
   * How long to wait before retrying if the data store goes down.
   */
  lazy val waitTime: Int = properties.getInt(TOPIC_PERSISTENT_CONSUMER_WAITTIME_KEY)

  /**
   * How many times to attempt reconnecting to Cassandra before giving up
   */
  lazy val retries: Int = properties.getInt(CASSANDRA_RETRIES_KEY)

  /** See [[KafkaConsumerCLIBase.consumerGroup()]] */
  override def consumerGroup(): KafkaConsumerGroupComponent[String, Message] = {
    val config = new ConsumerConfig(properties.props)
    Log.info(s"Creating consumer connector...")
    val cc = Consumer.create(config)
    Log.info(s"Created consumer connector: $cc")
    Log.info(s"Creating Balboa Consumer Group...")
    val g = new BalboaConsumerGroup(cc, topic, partitions, ds, waitTime, retries)
    Log.info(s"Group $g created")
    g
  }


  /** See [[KafkaConsumerCLIBase.optionParser()]] */
  override protected def optionParser(): joptsimple.OptionParser = {
    val p = super.optionParser()
    p.accepts("waitTime", "The length of time to wait when the data store is down.")
      .withRequiredArg()
      .ofType(classOf[Int])
      .defaultsTo(DEFAULT_PERSISTENT_CONSUMER_WAITTIME)
    p.accepts("cassandras", "Comma separated list of host:port cassandra nodes")
      .withRequiredArg()
      .ofType(classOf[String])
      .defaultsTo(LOCAL_CASSANDRA)
    p
  }

  /** See [[KafkaConsumerCLIBase.getProperties()]] */
  override protected def getProperties(args: Array[String]): Properties = {
    val original = super.getProperties(args)
    val set: OptionSet = optionParser().parse(args : _*)
    // Add the wait time parameter
    set.valueOf("waitTime") match {
      case waitTime: Integer =>
        original.put(TOPIC_PERSISTENT_CONSUMER_WAITTIME_KEY, waitTime.toString)
      case _ => // NOOP
    }
    set.valueOf("cassandras") match {
      case c: String =>
        original.put(CASSANDRA_SERVERS_KEY, c)
      case _ => // NOOP
    }
    original
  }

}
