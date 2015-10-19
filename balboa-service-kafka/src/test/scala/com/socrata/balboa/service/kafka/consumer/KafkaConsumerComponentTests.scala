package com.socrata.balboa.service.kafka.consumer

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ExecutorService, Executors, ScheduledExecutorService, TimeUnit}

import com.socrata.balboa.common.kafka.codec.StringCodec
import com.socrata.balboa.service.kafka.consumer.KafkaConsumerSpecSetup.TestKafkaConsumerSetup
import com.socrata.balboa.service.kafka.test.util.ConsumerTestUtil
import kafka.consumer.{ConsumerIterator, KafkaStream}
import org.mockito.Mockito
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, WordSpec}

import scala.collection.mutable

trait TestKafkaConsumerComponent extends KafkaConsumerComponent[String, String] {

  class TestKafkaConsumer extends KafkaConsumer {
    self: KafkaConsumerStreamProvider[String, String] with KafkaConsumerReadiness =>

    val queue: mutable.Queue[(String, String)] = mutable.Queue.empty[(String,String)]
    val retries: Int = 3

    /**
     * Consumes a Kafka Key-Message pair.
     *
     * @param k Key for the newly consumed message
     * @param m Message for the newly consumed message
     * @return Some(error) or None in case of success.
     */
    override def consume(k: String, m: String, attempts:Int = 0): Option[String] = {
      queue.enqueue((k,m))
      None
    }
  }
}

object KafkaConsumerSpecSetup extends MockitoSugar {

  /**
   * Reusable Mock Kafka Stream
   */
  trait MockKafkaStream {
    val mStream = mock[KafkaStream[String, String]]
    val mIterator = mock[ConsumerIterator[String, String]]
    Mockito.when(mStream.iterator()).thenReturn(mIterator)
  }

  /**
   * Reusable Kafka Consumer that tracts messages in a stream
   */
  trait TestKafkaConsumerSetup extends TestKafkaConsumerComponent with MockKafkaStream {
    var isReady = new AtomicBoolean(true)
    val wt = 600
    val r = 3
    val kafkaConsumer = new TestKafkaConsumer() with
      KafkaConsumerStreamProvider[String, String] with KafkaConsumerReadiness {
      override val stream: KafkaStream[String, String] = mStream
      override def ready: Boolean = isReady.get()
      override val waitTime: Long = wt
      override val retries: Int = r
    }
  }
}

/**
 * Kafka Consumer Component Unit test.
 */
class KafkaConsumerComponentSpec extends WordSpec with TestKafkaConsumerComponent with BeforeAndAfterEach {

  private val scodec = new StringCodec()

  override protected def afterEach(): Unit = super.afterEach()

  override protected def beforeEach(): Unit = super.beforeEach()

  "A Kafka Consumer Component" should {

    "not consume messages when the stream is empty" in new TestKafkaConsumerSetup {
      Mockito.when(mIterator.hasNext()).thenReturn(false)
      kafkaConsumer.start()
      assert(kafkaConsumer.queue.isEmpty, "No messages should have been added with a null")
    }

    "consume a single message exactly one is available" in new TestKafkaConsumerSetup {
      Mockito.when(mIterator.hasNext()).thenReturn(true, false)
      Mockito.when(mIterator.next()).thenReturn(ConsumerTestUtil.message("key", "message", scodec, scodec))
      kafkaConsumer.start()
      assert(kafkaConsumer.queue.size == 1, "No messages should have been added with a null")
      assert(kafkaConsumer.queue.exists(km => km._1.equals("key")  && km._2.equals("message")))
    }

    "consume multiple messages" in new TestKafkaConsumerSetup {
      Mockito.when(mIterator.hasNext()).thenReturn(true, true, true, true, true, false)
      Mockito.when(mIterator.next()).thenReturn(
        ConsumerTestUtil.message(null, "test_message1", scodec, scodec),
        ConsumerTestUtil.message(null, "test_message2", scodec, scodec),
        ConsumerTestUtil.message(null, "test_message3", scodec, scodec),
        ConsumerTestUtil.message(null, "test_message4", scodec, scodec),
        ConsumerTestUtil.message(null, "test_message5", scodec, scodec))

      kafkaConsumer.start()

      assert(kafkaConsumer.queue.size == 5, "Should have 5 elements")
      for (i <- 1 to 5) {
        assert(kafkaConsumer.queue.exists(km => km._1 == null && km._2 == s"test_message$i"),
          "The ingested message is not the same as the one " +
            "produced by the iterator")
      }
    }

    "wait until consumer is ready before continuing." in new TestKafkaConsumerSetup {
      Mockito.when(mIterator.hasNext()).thenReturn(true, false)
      Mockito.when(mIterator.next()).thenReturn(ConsumerTestUtil.message("test_key", "test_message", scodec, scodec))
      isReady.set(false)

      // Run the consumer and have it block until it is ready
      val s1: ExecutorService = Executors.newFixedThreadPool(1)
      s1.submit(kafkaConsumer)

      val service: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
      service.schedule(new Runnable {
        override def run(): Unit = {
          assert(kafkaConsumer.queue.isEmpty, "No messages should have been added ")
          isReady.set(true)
        }
      }, wt * 2, TimeUnit.MILLISECONDS)

      service.awaitTermination(wt * 3, TimeUnit.MILLISECONDS)
      s1.awaitTermination(wt, TimeUnit.MILLISECONDS)
      // isReady should be set to true by now.

      assert(kafkaConsumer.queue.size == 1, "Should have exactly one element")
      assert(kafkaConsumer.queue.exists(km => km._1 == "test_key" && km._2 == "test_message"), "The ingested message is not " +
        "the same as the one produced by the iterator")
    }
  }
}


