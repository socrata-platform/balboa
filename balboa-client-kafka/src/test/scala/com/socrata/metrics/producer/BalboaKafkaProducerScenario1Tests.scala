package com.socrata.metrics.producer

import com.socrata.balboa.common.kafka.codec.{BalboaMessageCodec, StringCodec}
import com.socrata.balboa.common.kafka.util.StagingAndRCEnvironment
import com.socrata.balboa.metrics.Message
import com.socrata.balboa.util.MetricsTestStuff
import com.socrata.integration.kafka.util.{BalboaClientTestUtils, BalboaMessageClientTestHarness}
import kafka.common.FailedToSendMessageException
import kafka.server.KafkaServer
import org.junit.Assert._
import org.junit.Test


/**
 * Behaviour testing Kafka Producer in a simulated environment.  This simulated environment is meant to resemble the
 * actual deployed staging and RC environments.
 */
class BalboaKafkaProducerScenario1Tests extends BalboaMessageClientTestHarness
with MetricsTestStuff.TestMessages {

  override val numPartitions: Int = StagingAndRCEnvironment.NUM_PARTITIONS
  override val replicationFactor: Int = StagingAndRCEnvironment.REPLICATION_FACTOR
  override val serverCount: Int = StagingAndRCEnvironment.SERVER_COUNT
  override val producerCount: Int = 2
  override val consumerGroupCount: Int = 3

  // For simplicity have each group will have one consumer.
  override val consumerCount: Int = 1

  override val topic: String = "balboa_kafka_producer_topic"

  @Test def testSendingSingleMessageIsEventuallyConsumed(): Unit = {
//    producers(0).send(oneElemMessage)
//
//    val messages: List[(String,Message)] = BalboaClientTestUtils.getKeysAndMessages[String,Message](1,
//      consumers(0).createMessageStreams[String, Message](
//        Map((topic, 1)), new StringCodec, new BalboaMessageCodec()))
//    assertEquals("Must have one element", 1, messages.size)
//    assert(messages.unzip._2.contains(oneElemMessage))
  }

//  /**
//   * The current specification defines that mutliple producers that happen to send the same message will result in the
//   * duplication for a specific consumer group.
//   */
//  @Test def testMultipleProducersSendTheSameMessageGetsDuplicates() =
//    downServerTestHelper(servers.slice(0,0), producers.slice(0,2), oneElemMessage)
//
//  @Test def testDownServerAndSendMessage() =
//    downServerTestHelper(servers.slice(0, 1), producers.slice(0,1), oneElemMessage, oneElemMessage, manyElemMessage)
//
//  @Test def testTwoDownServerAndSendMessage() =
//    downServerTestHelper(servers.slice(0, 2), producers.slice(0,1), oneElemMessage)
//
//  // TODO: Hmm this should fail. Replication factor 3 with 3 out of 4 servers down.????
//  @Test def testThreeDownServerAndSendMessage(): Unit =
//    downServerTestHelper(servers.slice(0, 3), producers.slice(0,1), oneElemMessage)
//
//  @Test def testFailedToSendMessageWithAllServerDown(): Unit = {
//    // Sending a message should fail because all the servers are down.
//    intercept[FailedToSendMessageException](downServerTestHelper(servers.slice(0,4), producers.slice(0,1), manyElemMessage))
//  }
//
//  def downServerTestHelper(downServers: Seq[KafkaServer],
//                           producers: Seq[GenericKafkaProducer[String,Message,StringCodec,BalboaMessageCodec]],
//                           inputMessages: Message*) = {
//    downServers.foreach(s => {
//      s.shutdown()
//      s.awaitShutdown()
//    })
//
//    producers.foreach(p => {
//      inputMessages.foreach(message => p.send(message))
//    })
//
//    consumerMap.foreach(x => {
//      val expectedSize = inputMessages.size * producers.size
//      // Little complex looking but this is just using the single consumer in each group to create a stream of messages
//      // and then pulls the 2 messages from the stream to verify and store in "messages"
//      val consumedMessages: List[(String,Message)] = BalboaClientTestUtils.getKeysAndMessages[String,Message](expectedSize,
//        x._2(0).createMessageStreams[String, Message](Map((topic, 1)), new StringCodec, new BalboaMessageCodec()))
//
//      // All the outgoing messages should be duplicated by a factor equal to the number of producers
//      assertEquals("Number of total messages sent not equal to the number of total messages received.",
//        expectedSize, consumedMessages.size)
//
//      // Each outgoing message must be contained in the eventually consumed message.
//      assert(inputMessages.forall(inMessage => {consumedMessages.unzip._2.contains(inMessage)}),
//        "All the input messages should be contained in the output.")
//    })
//  }
  
}
