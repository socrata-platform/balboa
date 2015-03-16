package com.socrata.balboa.common.kafka.consumer

import com.socrata.balboa.metrics.Message
import kafka.consumer.KafkaStream
import org.scalatest.mock.MockitoSugar

import scala.collection.mutable


/**
 * Test for [[BalboaConsumerGroup]].
 */
object BalboaConsumerGroupTests extends MockitoSugar {

  val NUM_PARTITIONS = 8

  trait MockKafkaStreams {
    var streams_wip: mutable.Buffer[KafkaStream[String, Message]] = mutable.Buffer.empty[KafkaStream[String, Message]]
    for (i <- Range(0,NUM_PARTITIONS)) {
      streams_wip.append(mock[KafkaStream[String, Message]])
    }
  }

}

