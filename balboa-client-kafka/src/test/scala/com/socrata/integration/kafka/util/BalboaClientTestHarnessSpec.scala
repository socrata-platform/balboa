package com.socrata.integration.kafka.util

import com.socrata.balboa.common.kafka.codec.StringCodec
import org.junit.Assert._
import org.junit.Test

import scala.collection.Map

/**
 * Test the Client test harness.
 */
class BalboaClientTestHarnessSpec extends StringClientTestHarness {
  override val producerCount: Int = 1
  override val serverCount: Int = 1
  override val consumerCount: Int = 1
  override val consumerGroupCount: Int = 1
  override val topic: String = "test_harness_topic"

  @Test def testCorrectNumberOfProducers() {
//    assertEquals("Test Harness has incorrect number of producers", producerCount, producers.size)
  }

//  @Test def testCorrectNumberOfServers() {
//    assertEquals("Test Harness has incorrect number of servers", serverCount, servers.size)
//  }
//
//  @Test def testCorrectNumberOfConsumers() {
//    assertEquals("Test Harness has incorrect number of consumer", consumerCount, consumers.size)
//    assertEquals("There should be only one consumer group", 1, consumerMap.keys.size)
//    assertEquals("There should be only one consumer for that specific consumer group", 1, consumerMap(0).size)
//  }
//
//  /**
//   * Simple test to show that we can consume messages.
//   */
//  @Test def testMessageEndToEndness(): Unit = {
//    val m = "Hello Balboa Client Test Harness parameterized World!"
//    producers(0).send(m)
//
//    // Example of how to consume messages and test we actually received them.
//    val messages: List[(String,String)] = BalboaClientTestUtils.getKeysAndMessages[String,String](1,
//      consumers(0).createMessageStreams[String, String](
//      Map((topic, 1)), new StringCodec, new StringCodec))
//    assert(messages.unzip._2.contains(m))
//  }

}
