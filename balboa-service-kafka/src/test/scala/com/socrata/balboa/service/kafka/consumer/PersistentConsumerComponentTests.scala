package com.socrata.balboa.service.kafka.consumer

import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean

import com.socrata.balboa.common.kafka.codec.StringCodec
import com.socrata.balboa.metrics.data.BalboaFastFailCheck
import com.socrata.balboa.service.kafka.consumer.PersistentConsumerComponentSetup.{ConfigurableTestPersistentKafkaConsumerSetup}
import com.socrata.balboa.service.kafka.test.util.{ConsumerTestUtil, SlowTest}
import kafka.consumer.{ConsumerIterator, KafkaStream}
import org.mockito.Mockito
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, WordSpec}

import scala.collection.mutable


trait TestPersistentKafkaConsumerComponent extends PersistentKafkaConsumerComponent[String, String] {
  class TestPersistentKafkaConsumer(maxFailures: Int, maxRetries: Int) extends PersistentKafkaConsumer {
    self: KafkaConsumerStreamProvider[String, String] with PersistentKafkaConsumerReadiness =>

    val queue: mutable.Queue[(String, String)] = mutable.Queue.empty[(String,String)]

    var numFailures = 0
    val retries = maxRetries

    /**
     * Attempt to persist a message.
     *
     * @param message entity to persist.
     * @throws IOException if there was a recoverable error.
     */
    override protected def persist(key: String, message: String): Unit = {
      if (numFailures < maxFailures) {
        numFailures += 1
        throw new IOException("Mocked failure")
      }
      queue.enqueue((key, message))
    }
  }
}

object PersistentConsumerComponentSetup extends MockitoSugar {

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
  class ConfigurableTestPersistentKafkaConsumerSetup(maxFailures: Int, maxRetries: Int) extends TestPersistentKafkaConsumerComponent with MockKafkaStream {
    var isReady = new AtomicBoolean(true)
    val wt = 500
    val kafkaConsumer = new TestPersistentKafkaConsumer(maxFailures, maxRetries) with
      KafkaConsumerStreamProvider[String, String] with PersistentKafkaConsumerReadiness {
      override val waitTime: Long = wt
      override val stream: KafkaStream[String, String] = mStream
    }
  }
}

/**
 * Persistent Consumer Unit Test.
 */
class PersistentConsumerComponentSpec extends WordSpec with BeforeAndAfterEach {

  override protected def afterEach(): Unit = BalboaFastFailCheck.getInstance().markSuccess()

  private val scodec = new StringCodec()

//  "A Persistent Consumer Component" should {
//
//    "retry when initial persist fails" taggedAs SlowTest in new ConfigurableTestPersistentKafkaConsumerSetup(1,2) {
//      Mockito.when(mIterator.hasNext()).thenReturn(true, false)
//      Mockito.when(mIterator.next()).thenReturn(ConsumerTestUtil.message("test_key", "test_message", scodec, scodec))
//
//
//      kafkaConsumer.start()
//      assert(kafkaConsumer.queue.size == 1, "Should have exactly one element")
//      assert(kafkaConsumer.queue.exists(km => km._1 == "test_key" && km._2 == "test_message"),
//        "Message does not eventually become consumed.")
//    }
//
//    "throws exception when it retries more than 2 times" taggedAs SlowTest in new ConfigurableTestPersistentKafkaConsumerSetup(2,2) {
//      Mockito.when(mIterator.hasNext()).thenReturn(true, false)
//      Mockito.when(mIterator.next()).thenReturn(ConsumerTestUtil.message("test_key", "test_message", scodec, scodec))
//
//      intercept[IOException] {
//        kafkaConsumer.start()
//        assert(kafkaConsumer.queue.size == 0, "Should have no one elements")
//      }
//    }
//
//  }
}