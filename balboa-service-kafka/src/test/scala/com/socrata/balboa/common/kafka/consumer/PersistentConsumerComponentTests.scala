package com.socrata.balboa.common.kafka.consumer

import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean

import com.socrata.balboa.common.kafka.codec.StringCodec
import com.socrata.balboa.common.kafka.consumer.PersistentConsumerComponentSetup.TestPersistentKafkaConsumerSetup
import com.socrata.balboa.common.kafka.test.util.{ConsumerTestUtil, SlowTest}
import com.socrata.balboa.metrics.data.BalboaFastFailCheck
import kafka.consumer.{ConsumerIterator, KafkaStream}
import org.mockito.Mockito
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, WordSpec}

import scala.collection.mutable

trait TestPersistentKafkaConsumerComponent extends PersistentKafkaConsumerComponent[String, String] {

  class TestPersistentKafkaConsumer extends PersistentKafkaConsumer {
    self: PersistentKafkaExternalComponents[String, String] with PersistentKafkaConsumerReadiness =>

    val queue: mutable.Queue[(String, String)] = mutable.Queue.empty[(String,String)]

    val maxFailures = 3
    var numFailures = 0

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
  trait TestPersistentKafkaConsumerSetup extends TestPersistentKafkaConsumerComponent with MockKafkaStream {
    var isReady = new AtomicBoolean(true)
    val waitTime = 500
    val kafkaConsumer = new TestPersistentKafkaConsumer() with
      PersistentKafkaExternalComponents[String, String] with PersistentKafkaConsumerReadiness {
      override val waitTime: Long = waitTime
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

  "A Persistent Consumer Component" should {

    "retry when initial persist fails" taggedAs SlowTest in new TestPersistentKafkaConsumerSetup {
      Mockito.when(mIterator.hasNext()).thenReturn(true, false)
      Mockito.when(mIterator.next()).thenReturn(ConsumerTestUtil.message("test_key", "test_message", scodec, scodec))
      kafkaConsumer.run()
      assert(kafkaConsumer.queue.size == 1, "Should have exactly one element")
      assert(kafkaConsumer.queue.exists(km => km._1 == "test_key" && km._2 == "test_message"),
        "Message does not eventually become consumed.")
    }

  }
}