package com.socrata.balboa.service.kafka.consumer

import com.socrata.balboa.metrics.Message
import com.socrata.balboa.metrics.data.DataStore
import com.socrata.balboa.common.kafka.codec.{BalboaMessageCodec, StringCodec}
import kafka.consumer.{ConsumerConnector, Consumer, ConsumerConfig, KafkaStream}

/**
 * A [[KafkaConsumerGroupComponent[String, Message]] that is meant to handle Balboa specific traffic.
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

  def this(consumerConfig: ConsumerConfig,
           topic: String,
           partitionsForTopic: Int,
           dataStore: DataStore,
           waitTime: Long) = this(Consumer.create(consumerConfig), topic, partitionsForTopic, dataStore, waitTime)

  /**
   * Create the streams on demand.
   */
  lazy val streams: List[KafkaStream[String,Message]] = connector.createMessageStreams[String, Message](
    Map((topic, partitionsForTopic)), new StringCodec(), new BalboaMessageCodec())(topic)

  /**
   * Each consumer will have each own individual stream to consume.
   *
   * @return A list of Kafka Consumers that belong exclusively to this group.
   */
  override val consumers: List[KafkaConsumer] = this.streams.map(s => {
    val d = this.dataStore
    val w = this.waitTime
    new BalboaConsumer() with DataStoreConsumerExternalComponents with PersistentKafkaConsumerReadiness {
      lazy val dataStore: DataStore = d
      lazy val waitTime: Long = w
      lazy val stream = s
    }
  })

  /**
   * Shutdowns all the consumer threads but stopping all the streams via the consumer connector.
   */
  override def stop(): Option[Exception] = {
    try {
      connector.shutdown()
      super.stop()
    } catch {
      case e: Exception => Some(e)
    }
  }

  override def toString: String = {
    val c = this.getClass
    val size = consumers.size
    s"$c topic: $topic number of consumers: $size data store: $dataStore"
  }
}
