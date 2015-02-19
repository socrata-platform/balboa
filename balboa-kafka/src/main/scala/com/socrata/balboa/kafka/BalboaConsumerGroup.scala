package com.socrata.balboa.kafka

import java.util.concurrent.{Executors, ExecutorService}

import com.socrata.balboa.kafka.consumer.TestConsumer
import kafka.consumer.{KafkaStream, ConsumerConnector, Consumer, ConsumerConfig}

import scala.collection.Map
import scala.collection.immutable.HashMap

/**
 * Created by Michael Hotan, michael.hotan@socrata.com
 *
 * ADT that manages the connection with zookeeper nodes and consumption of Kafka Messages.  ADT attempts to utilize a
 * pure functional
 *
 * Note: Most likely won't
 */
sealed trait BalboaConsumerGroup
case class ::(consumer: ConsumerConnector, topic: String) extends BalboaConsumerGroup

/**
 * `BalboaKafka` Companion object
 * */
object BalboaConsumerGroup {

  /**
   * Initialize a Balboa Kafka Consumer Service using a set of configuration
   *
   * @param consumerConfig Consumer configurations
   * @return Balboa Kafka instance
   */
  def init(consumerConfig: ConsumerConfig, topic: String): BalboaConsumerGroup = {
    (consumerConfig, topic) match {
      case _=> ::(Consumer.create(consumerConfig), topic)
    }
  }

  /**
   * Return the streams for a given Balboa Consumer Group.
   *
   * Note: Not pure function. How to make pure?
   *
   * @param numThreads Number of threads to spin off.
   */
  def getStreams(cg: BalboaConsumerGroup, numThreads: Int): Option[List[KafkaStream[Array[Byte],Array[Byte]]]] = cg
  match {
    case ::(cons,topic) =>
      var topicCountMap: Map[String, Int] = new HashMap[String, Int]
      topicCountMap += (topic -> numThreads)
      val consMap: Map[String, List[KafkaStream[Array[Byte],Array[Byte]]]] = cons.createMessageStreams(topicCountMap)
      consMap.get(topic)
  }

}