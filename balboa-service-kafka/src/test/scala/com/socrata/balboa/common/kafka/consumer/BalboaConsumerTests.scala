package com.socrata.balboa.common.kafka.consumer

import java.util.concurrent.atomic.AtomicBoolean

import com.socrata.balboa.common.kafka.codec.{BalboaMessageCodec, StringCodec}
import com.socrata.balboa.common.kafka.consumer.BalboaConsumerComponentSetup.TestBalboaKafkaConsumerSetup
import com.socrata.balboa.common.kafka.test.util.ConsumerTestUtil
import com.socrata.balboa.metrics.Message
import com.socrata.balboa.metrics.data.{BalboaFastFailCheck, DataStore}
import com.socrata.balboa.metrics.impl.JsonMessage
import kafka.consumer.{ConsumerIterator, KafkaStream}
import kafka.message.MessageAndMetadata
import org.mockito.Mockito
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, WordSpec}

object BalboaConsumerComponentSetup extends MockitoSugar {

  /**
   * Reusable Mock Kafka Stream
   */
  trait MockKafkaStream {
    val mStream = mock[KafkaStream[String, Message]]
    val mIterator = mock[ConsumerIterator[String, Message]]
    val mDS = mock[DataStore]
    Mockito.when(mStream.iterator()).thenReturn(mIterator)
  }

  /**
   * Reusable Kafka Consumer that tracts messages in a stream
   */
  trait TestBalboaKafkaConsumerSetup extends BalboaConsumerComponent with MockKafkaStream {
    var isReady = new AtomicBoolean(true)
    val waitTime = 500
    val balboaConsumer = new BalboaConsumer() with
      BalboaConsumerExternalComponents with PersistentKafkaConsumerReadiness {
      override val waitTime: Long = waitTime
      override val stream: KafkaStream[String, Message] = mStream
      override val dataStore: DataStore = mDS
    }
  }
}

/**
* Unit and Integration test for Balboa Consumer
*/
class BalboaConsumerSpec extends WordSpec with BeforeAndAfterEach {

  val stringCodec = new StringCodec()
  val messageCodec = new BalboaMessageCodec()

  override protected def afterEach(): Unit = BalboaFastFailCheck.getInstance().markSuccess()

  "A Balboa Consumer Component" should {

    "persist 1 entry to the data store" in new TestBalboaKafkaConsumerSetup {
      Mockito.when(mIterator.hasNext()).thenReturn(true, false)
      val json: JsonMessage = ConsumerTestUtil.testJSONMetric("test_metric", 1)
      val mm = ConsumerTestUtil.message(null, json, stringCodec, messageCodec)
      Mockito.when(mIterator.next()).thenReturn(mm)
      balboaConsumer.run()
      Mockito.verify(mDS).persist(json.getEntityId, json.getTimestamp, json.getMetrics)
    }

    "persist multiple entries to the data store" in new TestBalboaKafkaConsumerSetup {
      Mockito.when(mIterator.hasNext()).thenReturn(true, true, true, true, true, false)
      var jsons: List[JsonMessage] = List[JsonMessage]()
      var mms = List[MessageAndMetadata[String, Message]]()
      for (i <- 1 to 5) {
        val json = ConsumerTestUtil.testJSONMetric(s"test_metric_$i", i)
        jsons = jsons :+ json
        mms = mms :+ ConsumerTestUtil.message(null, json, stringCodec, messageCodec)
      }

      Mockito.when(mIterator.next()).thenReturn(mms(0), mms(1), mms(2), mms(3), mms(4))
      balboaConsumer.run()
      jsons.foreach(j => Mockito.verify(mDS).persist(j.getEntityId, j.getTimestamp, j.getMetrics))
    }

    // TODO This test is temprarily not working.  Its a mock object thing.
    // Currently the test in Persistent Consumer Spec test failing to persist.
//    "handle temporarily down datastore" in new TestBalboaConsumer {
//      Mockito.when(mIterator.hasNext()).thenReturn(true, false)
//      val json: JsonMessage = ConsumerTestUtil.testJSONMetric("test_metric", 1)
//      val mm = ConsumerTestUtil.message(json)
//      Mockito.when(mIterator.next()).thenReturn(mm)
//      Mockito.when(mDS.persist(json.getEntityId, json.getTimestamp, json.getMetrics)).thenThrow(classOf[IOException])
//        .thenReturn(())
//      balboaConsumer.run()
//      Mockito.verify(mDS).persist(json.getEntityId, json.getTimestamp, json.getMetrics)
//    }
  }
}
