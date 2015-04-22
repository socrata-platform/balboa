package com.socrata.balboa.service.kafka.consumer

import java.util.concurrent.Future
import com.socrata.balboa.util.MetricsTestStuff
import MetricsTestStuff.TestMessages
import com.socrata.balboa.metrics.Message
import com.socrata.balboa.service.kafka.test.util.StagingAndRCServiceTestHarness
import com.socrata.balboa.util.MetricsTestStuff
import kafka.consumer.{ConsumerConfig, ConsumerTimeoutException}
import kafka.utils.TestUtils
import org.junit.Assert._
import org.junit.Test


/**
 * Test for [[BalboaConsumerGroup]].
 */
class BalboaConsumerGroupIntegrationTest extends StagingAndRCServiceTestHarness with TestMessages {

  var consumerGroup1: KafkaConsumerGroupComponent[String, Message] = null

  override val producerCount: Int = 1

  override val CONSUMER_TIMEOUT_MS = 1000

  override def tearDown(): Unit = {
    consumerGroup1.stop()
    dataStore.metricMap.clear()
    super.tearDown()
  }

  override def setUp(): Unit = {
    super.setUp()
    consumerGroup1 = genConsumerGroup(new ConsumerConfig(
      TestUtils.createConsumerProperties(zkConnect, "group_1", "consumer_1")) {
      // NOTE: Important in testing environments we need consumers to terminate when there "appears" to be no more messages.
      // For some reason we have to over the time out
      override val consumerTimeoutMs = CONSUMER_TIMEOUT_MS
    })
    assertEquals("Consumer Group size is not equivalent to the number of partitions", numPartitions, consumerGroup1.consumers.size)
  }

  /**
   * When there are no messages sent then all the consumers should timeout.
   */
  @Test def testConsumerReceivesNothingIfNoMessagesAreSentAndFailsNormally(): Unit = {
    testMessages(Seq.empty)
  }

  /**
   * Make sure one message makes it end to end from [[com.socrata.metrics.producer.GenericKafkaProducer]] to
   * [[BalboaConsumerGroup]]
   */
  @Test def testConsumerReceivesOneSentMessage() {
    testMessages(Seq(oneElemMessage))
  }

  /**
   * Make sure multiple messages make end to end and maintain the correct state.
   */
  @Test def testManyMessages(): Unit = {
    // TODO Identify the number of messages that best resembles the actual number the traffic through real environments.
    testMessages(Range(0, 10000).map(i => MetricsTestStuff.message(s"entity_$i",
      MetricsTestStuff.metrics((s"metric_$i", MetricsTestStuff.metric(i))), i)))
  }

  private def testMessages(messages: Seq[Message]): Unit = {
    messages.foreach(m => producers(0).send(m))
    val futures: List[Future[_]] = consumerGroup1.start()
    futures.foreach(f => f.get() match {
      case Some(cte: ConsumerTimeoutException) => // Success
      case Some(e: Exception) => fail("Caught unexpected exception", e)
      case None => fail(s"Expection expected but not caught")
    })
    val expectedSize = messages.size
    assertEquals(expectedSize, dataStore.metricMap.size())
    assertTrue(s"All $expectedSize metrics entries were not found in $dataStore",
      messages.forall(m => dataStore.metricMap.containsKey(m.getEntityId)
        && dataStore.metricMap.get(m.getEntityId).equals(m.getMetrics)))
  }

}

