package com.socrata.balboa.service.kafka

import java.util.Properties

import com.socrata.balboa.metrics.Message
import com.socrata.balboa.metrics.data.{DataStore, DataStoreFactory}
import com.socrata.balboa.service.kafka.consumer.{BalboaConsumerGroup, KafkaConsumerGroupComponent}
import joptsimple.OptionSet
import kafka.consumer.ConsumerConfig

/**
 * Balboa Kafka Main Application:
 *
 * Entry point for Balboa Kafka Consumer.  This application starts a service that listens
 * on a configurable list of ZooKeeper Host Ports.  This service currently listens for all published Tenant Metrics.
 */
object BalboaKafkaConsumerCLI extends KafkaConsumerCLIBase[String, Message] {

  private val TOPIC_PERSISTENT_CONSUMER_WAITTIME_KEY = "balboa.kafka.consumer.persistent.waittime"
  // Default the time we wait for a data store to
  val DEFAULT_PERSISTENT_CONSUMER_WAITTIME = 1000

  // Retrieve the default data store.
  lazy val ds: DataStore = DataStoreFactory.get

  /**
   * How long to wait before retrying if the data store goes down.
   */
  lazy val waitTime: Int = properties.getInt(TOPIC_PERSISTENT_CONSUMER_WAITTIME_KEY)

  /** See [[KafkaConsumerCLIBase.consumerGroup()]] */
  override def consumerGroup(): KafkaConsumerGroupComponent[String, Message] = new BalboaConsumerGroup(
    new ConsumerConfig(properties.props), topic, partitions, ds, waitTime)

  /** See [[KafkaConsumerCLIBase.optionParser()]] */
  override protected def optionParser(): joptsimple.OptionParser = {
    val p = super.optionParser()
    p.accepts("waitTime", "The length of time to wait when the data store is down.").withRequiredArg()
      .ofType(classOf[Int]).defaultsTo(DEFAULT_PERSISTENT_CONSUMER_WAITTIME)
    p
  }

  /** See [[KafkaConsumerCLIBase.getProperties()]] */
  override protected def getProperties(args: Array[String]): Properties = {
    val original = super.getProperties(args)
    val set: OptionSet = optionParser().parse(args : _*)
    // Add the wait time parameter
    if (!set.has("configFile")) {
      set.valueOf("waitTime") match {
        case waitTime: Integer =>
          original.put(TOPIC_PERSISTENT_CONSUMER_WAITTIME_KEY, waitTime.toString)
        case _ => throw new ClassCastException()
      }
    }
    original
  }

}
