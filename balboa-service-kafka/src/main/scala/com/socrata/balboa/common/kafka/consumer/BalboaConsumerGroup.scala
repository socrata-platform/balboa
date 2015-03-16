package com.socrata.balboa.common.kafka.consumer

import com.socrata.balboa.common.kafka.codec.{BalboaMessageCodec, StringCodec}
import com.socrata.balboa.metrics.Message
import com.socrata.balboa.metrics.data.DataStore
import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}

/**
 * A [[KafkaConsumerGroup[String, Message]] that is meant to handle Balboa specific traffic.
 */
sealed trait BalboaConsumerGroup extends KafkaConsumerGroup[String, Message] with BalboaConsumerComponent

/**
 * Class that handles the construction and preparation of individual [[BalboaConsumerComponent.BalboaConsumer]]
 *
 * @param streams Kafka Streams for a particular topic.
 * @param dataStore The datastore to persist data to.
 * @param waitTime How to to wait for a down datastore (ms)
 */
sealed case class ::(streams: List[KafkaStream[String,Message]],
                     dataStore: DataStore,
                     waitTime: Long) extends BalboaConsumerGroup {

  override def consumers: List[KafkaConsumer] = streams.map(s => new BalboaConsumer()
    with BalboaConsumerExternalComponents with PersistentKafkaConsumerReadiness {
    lazy val dataStore: DataStore = dataStore
    lazy val waitTime: Long = waitTime
    lazy val stream = s
  })
}

object BalboaConsumerGroup {

  /**
   * Currently, [[BalboaConsumerGroup]]s are used to consume exactly one topic at a time.  This is a single entry point
   * to produce a Consumer Group that received Balboa Metric Messages and persist them into a parameterized data store.
   *
   * @param consumerConfig See [[ConsumerConfig]]
   * @param topic Kafka topic
   * @param partitionsForTopic Number of partition within a given Kafka Cluster for a particular topic
   * @param dataStore The data store to write Metrics to.
   * @param waitTime How long to wait in case of a down data store.
   * @return Some([[BalboaConsumerGroup]]) or None in case of invalid arguments.
   */
  def create(consumerConfig: ConsumerConfig,
             topic: String,
             partitionsForTopic: Int,
             dataStore: DataStore,
             waitTime: Long): Option[KafkaConsumerGroup[String, Message]] = {
    (consumerConfig, topic.trim, partitionsForTopic) match {
      case (cc: ConsumerConfig, t: String, ts: Int) if partitionsForTopic > 0 && topic.length > 0 =>
        val streams: List[KafkaStream[String,Message]] = Consumer.create(consumerConfig)
          .createMessageStreams[String, Message](Map((topic, partitionsForTopic)),
            new StringCodec(), new BalboaMessageCodec())(topic)
        Some(::(streams, dataStore, waitTime))
      case _ => None
    }
  }

}
