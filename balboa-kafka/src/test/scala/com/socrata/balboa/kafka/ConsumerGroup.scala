package com.socrata.balboa.kafka

import java.util.concurrent.Executors

import com.socrata.balboa.kafka.consumer.KafkaConsumer
import kafka.consumer.{KafkaStream, ConsumerConnector, Consumer, ConsumerConfig}

import scala.collection.Map

/**
 * A group of Kafka Consumers that handle kafka message ingestion, translation, and potentially storage.
 */
trait ConsumerGroup
sealed case class ::(consumer: ConsumerConnector, topic: String, threads: Int) extends ConsumerGroup

object ConsumerGroup {

  /**
   * Initialize this consumer group for a specific topic.
   */
  def init(consumerConfig: ConsumerConfig, topic: String, threads: Int): Option[ConsumerGroup] = {
    (consumerConfig, topic.trim, threads) match {
      case (cc: ConsumerConfig, t: String, ts: Int) if threads > 0 && topic.length > 0 => Some(::(Consumer.create(cc),
        t, threads))
      case _ => None
    }
  }

  /**
   * Starts a set of worker threads that ingest data from a set of kafka streams.
   *
   * @param cg Consumer Group
   * @param creator A function that takes a KafkaStream and returns a KafkaConsumer for that stream
   * @tparam A KafkaConsumer type
   * @return Some: Error, None: Success
   */
  def runWorkerThreads[A, B <: KafkaConsumer[A]](cg: ConsumerGroup, creator: (KafkaStream[Array[Byte], Array[Byte]]) =>
    B):
  Option[String] = cg match {
    case (c: ::) =>
      val topicCountMap: Map[String, Int] = Map(c.topic -> c.threads)
      val consMap: Map[String, List[KafkaStream[Array[Byte],Array[Byte]]]] = c.consumer.createMessageStreams(topicCountMap)
      consMap.get(c.topic) match {
        case Some(streams) =>
          val executor = Executors.newFixedThreadPool(c.threads)
          streams.foreach(s => executor.submit(creator(s)))
          None
        case None =>
          val topic = c.topic
          Some(s"No available streams for $topic")
      }
    case _ => Some(s"Unable to create streams with provided consumer group: $cg")
  }

}