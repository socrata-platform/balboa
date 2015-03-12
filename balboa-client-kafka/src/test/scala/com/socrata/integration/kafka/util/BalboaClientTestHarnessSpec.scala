package com.socrata.integration.kafka.util

import com.socrata.balboa.kafka.codec.StringCodec
import kafka.consumer.{ConsumerIterator, KafkaStream}
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
  override val topic: String = "test_harness_topic"

  @Test def testCorrectNumberOfProducers() {
    assertEquals("Test Harness has incorrect number of producers", producerCount, producers.size)
  }

  @Test def testCorrectNumberOfServers() {
    assertEquals("Test Harness has incorrect number of servers", serverCount, servers.size)
  }

  @Test def testCorrectNumberOfConsumers() {
    assertEquals("Test Harness has incorrect number of consumer", consumerCount, consumers.size)
  }

//  @Test def testMessageEndToEndness(): Unit = {
//    val m = "Hello Balboa Client Test Harness parameterized World!"
//    producers(0).send(m)
//    val streams: Map[String,List[KafkaStream[String,String]]] = consumers(0).createMessageStreams[String, String](
//      Map((topic, 1)), new StringCodec, new StringCodec)
//    val stream: KafkaStream[String, String] =  streams.get(topic).get(0)
//    val iter: ConsumerIterator[String, String] = stream.iterator()
//    assert(iter.hasNext(), "There is one pending message for the consumer")
//    val mam = iter.next()
//    assertNotNull("Invalid Message and MetaData", mam)
//    assertEquals("Correctly received message", m, mam.message())
//  }

}
