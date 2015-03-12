package com.socrata.balboa.kafka.consumer

import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ExecutorService, Executors, ScheduledExecutorService, TimeUnit}

import com.socrata.balboa.kafka.codec.StringCodecLike
import com.socrata.balboa.kafka.consumer.KafkaConsumerSpecSetup.{ConsumerDefaults, MockKafkaStream, TestKafkaConsumer}
import com.socrata.balboa.kafka.consumer.PersistentConsumerSpecSetup.TestPersistentConsumer
import com.socrata.balboa.kafka.test.util.{ConsumerTestUtil, MultiThreadedTest, SlowTest}
import com.socrata.balboa.metrics.data.BalboaFastFailCheck
import kafka.consumer.{ConsumerIterator, KafkaStream}
import org.mockito.Mockito
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, WordSpec}

import scala.language.reflectiveCalls


object KafkaConsumerSpecSetup extends MockitoSugar {

  trait ConsumerDefaults {
    // Arbitrary default
    val waitTime = 2000
  }

  /**
   * Reusable Mock Kafka Stream
   */
  trait MockKafkaStream {
    val mStream = mock[KafkaStream[Array[Byte], Array[Byte]]]
    val mIterator = mock[ConsumerIterator[Array[Byte], Array[Byte]]]
    Mockito.when(mStream.iterator()).thenReturn(mIterator)
  }

  /**
   * Reusable Kafka Consumer that tracts messages in a stream
   */
  trait TestKafkaConsumer extends MockKafkaStream with ConsumerDefaults {
    var messages = List[String]()
    val isReady = new AtomicBoolean(true)
    val kafkaConsumer: KafkaConsumer[String] = new KafkaConsumer[String](mStream, waitTime) with StringCodecLike {

      /**
       * Function that handles the reception of new Messages
       */
      override protected def onMessage(key: Option[String], message: String): Unit = messages = messages :+ message

      override protected def ready: Boolean = isReady.get()
    }
  }
}

/**
 * Unit Test for Kafka Consumer
 */
class KafkaConsumerSpec extends WordSpec {

  "A KafkaConsumer" should {

    "ignore null messages" in new TestKafkaConsumer {
      Mockito.when(mIterator.hasNext()).thenReturn(true, false)
      Mockito.when(mIterator.next()).thenReturn(null)
      kafkaConsumer.run()
      assert(messages.isEmpty, "No messages should have been added with a null")
    }

    "Consume 1 message" in new TestKafkaConsumer {
      Mockito.when(mIterator.hasNext()).thenReturn(true, false)
      Mockito.when(mIterator.next()).thenReturn(ConsumerTestUtil.message("test_key", "test_message"))
      kafkaConsumer.run()
      assert(messages.size == 1, "Should have exactly one element")
      assert(messages.contains(ConsumerTestUtil.combine("test_key", "test_message")), "The ingested message is not " +
        "the " +
        "same as the one " +
        "produced by the iterator")
    }

    "Consume 1 message even if there is a null key" in new TestKafkaConsumer {
      Mockito.when(mIterator.hasNext()).thenReturn(true, false)
      Mockito.when(mIterator.next()).thenReturn(ConsumerTestUtil.message(null, "test_message"))
      kafkaConsumer.run()
      assert(messages.size == 1, "Should have exactly one element")
      assert(messages.contains(ConsumerTestUtil.combine(null, "test_message")), "The ingested message is not the same" +
        " as the one " +
        "produced by the iterator")
    }

    "Fail if message is null" in new TestKafkaConsumer {

      Mockito.when(mIterator.hasNext()).thenReturn(true, false)
      Mockito.when(mIterator.next()).thenReturn(ConsumerTestUtil.message("test_key", null))
      kafkaConsumer.run()
      assert(messages.isEmpty, "No messages should have been added with a null message")
    }

    "Consume multiple messages" in new TestKafkaConsumer {
      Mockito.when(mIterator.hasNext()).thenReturn(true, true, true, true, true, false)
      Mockito.when(mIterator.next()).thenReturn(
        ConsumerTestUtil.message(null, "test_message1"),
        ConsumerTestUtil.message(null, "test_message2"),
        ConsumerTestUtil.message(null, "test_message3"),
        ConsumerTestUtil.message(null, "test_message4"),
        ConsumerTestUtil.message(null, "test_message5"))

      kafkaConsumer.run()

      assert(messages.size == 5, "Should have 5 elements")
      for (i <- 1 to 5) {
        assert(messages.contains(ConsumerTestUtil.combine(null, s"test_message$i")), "The ingested message is not the" +
          " same as the one " +
          "produced by the iterator")
      }
    }

    "initially wait if the consumer is not ready" taggedAs(SlowTest,MultiThreadedTest) in new TestKafkaConsumer {
      Mockito.when(mIterator.hasNext()).thenReturn(true, false)
      Mockito.when(mIterator.next()).thenReturn(ConsumerTestUtil.message("test_key", "test_message"))
      isReady.set(false)

      // Run the consumer and have it block until it is ready
      val s1: ExecutorService = Executors.newFixedThreadPool(1)
      s1.submit(kafkaConsumer)

      val service: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
      service.schedule(new Runnable {
        override def run(): Unit = {
          assert(messages.isEmpty, "No messages should have been added ")
          isReady.set(true)
        }
      }, waitTime * 2, TimeUnit.MILLISECONDS)

      service.awaitTermination(waitTime * 3, TimeUnit.MILLISECONDS)
      s1.awaitTermination(waitTime, TimeUnit.MILLISECONDS)
      // isReady should be set to true by now.

      assert(messages.size == 1, "Should have exactly one element")
      assert(messages.contains(ConsumerTestUtil.combine("test_key", "test_message")), "The ingested message is not " +
        "the same as the one produced by the iterator")
    }
  }
}

object PersistentConsumerSpecSetup extends MockitoSugar {

  trait TestPersistentConsumer extends MockKafkaStream with ConsumerDefaults {
    var messages = List[String]()
    val persConsumer = new PersistentConsumer[String](mStream, waitTime) with StringCodecLike {

      var failedOnce: Boolean = false

      override protected def persist(message: String): Unit = messages = {
        if (!failedOnce) {
          failedOnce = true
          throw new IOException()
        }
        messages :+ message
      }

    }
  }
}

/**
 * Unit Test for Persistent Consumer
 */
class PersistentConsumerSpec extends WordSpec with BeforeAndAfter {

  val fastFailCheck = BalboaFastFailCheck.getInstance()

  after {
    // Always reset the fast fail check
    fastFailCheck.markSuccess()
  }

  "A PersistentConsumer" should {

    "retry when initial persist fails" taggedAs SlowTest in new TestPersistentConsumer {
      Mockito.when(mIterator.hasNext()).thenReturn(true, false)
      Mockito.when(mIterator.next()).thenReturn(ConsumerTestUtil.message("test_key", "test_message"))

      persConsumer.run()

      assert(messages.size == 1, "Should have exactly one element")
      assert(messages.contains(ConsumerTestUtil.combine("test_key", "test_message")), "The ingested message is not " +
        "the same as the one produced by the iterator")
    }

  }

}
