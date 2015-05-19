package com.socrata.balboa.service.kafka.consumer

import com.socrata.balboa.common.kafka.codec.{BalboaMessageCodec, StringCodec}
import com.socrata.balboa.metrics.Message
import com.socrata.balboa.metrics.data.DataStore
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector, KafkaStream}
import org.apache.commons.logging.LogFactory

/**
 * A [[KafkaConsumerGroupComponent[String, Message]] that is meant to handle Balboa specific Kafka message traffic.
 */
sealed trait BalboaConsumerGroupLike extends KafkaConsumerGroupComponent[String, Message] with DataStoreConsumerComponent

/**
 * Currently, [[BalboaConsumerGroup]]s are used to consume exactly one topic at a time.  This is a single entry point
 * to produce a Consumer Group that received Balboa Metric Messages and persist them into a parameterized data store.
 *
 * @param connector Connect that will produce
 * @param topic Kafka topic to subscribe to.
 * @param partitionsForTopic Number of partition within a given Kafka Cluster for a particular topic
 * @param dataStore The data store to write Metrics to.
 * @param waitTime How long to wait(ms) in case of a down data store.
 * @return Some([[BalboaConsumerGroup]]) or None in case of invalid arguments.
 */
case class BalboaConsumerGroup(connector: ConsumerConnector,
                               topic: String,
                               partitionsForTopic: Int,
                               dataStore: DataStore,
                               waitTime: Long) extends BalboaConsumerGroupLike {

  private val Log = LogFactory.getLog(this.getClass)

  def this(consumerConfig: ConsumerConfig,
           topic: String,
           partitionsForTopic: Int,
           dataStore: DataStore,
           waitTime: Long) = {
    this(Consumer.create(consumerConfig), topic, partitionsForTopic, dataStore, waitTime)
  }

  /**
   * Create the streams on demand.
   */
  lazy val streams: List[KafkaStream[String,Message]] = {
    Log.info(s"Initializing Kafka Streams for topic $topic using $partitionsForTopic")
    connector.createMessageStreams[String, Message](Map((topic, partitionsForTopic)), new StringCodec(), new BalboaMessageCodec())(topic)
  }

  /**
   * Each consumer will have each own individual stream to consume.
   *
   * @return A list of Kafka Consumers that belong exclusively to this group.
   */
  override val consumers: List[KafkaConsumer] = {
    Log.info("Initializing individual BalboaConsumers")
    this.streams.map(s => {
      val d = this.dataStore
      val w = this.waitTime
      new BalboaConsumer() with DataStoreConsumerExternalComponents with PersistentKafkaConsumerReadiness {
        lazy val dataStore: DataStore = d
        lazy val waitTime: Long = w
        lazy val stream = s
      }
    })
  }


  /**
   * Shutdowns all the consumer threads but stopping all the streams via the consumer connector.
   */
  override def stop(): Option[Exception] = {
    try {
      Log.info(s"Stopping ${this.toString}.")
      connector.shutdown()
      super.stop()
    } catch {
      case e: Exception =>
        Log.warn(s"Exception caught while attempting to shutdown ${this.toString}. Exception: ${e}")
        Some(e)
    }
  }

  override def toString: String = s"[${this.getClass}, topic: $topic, " +
    s"number of consumers: ${consumers.size}, datastore: $dataStore]"
}
