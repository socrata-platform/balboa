package com.socrata.balboa.kafka.consumer

import com.socrata.balboa.kafka.consumer.BalboaConsumeSpecSetup.TestBalboaConsumer
import com.socrata.balboa.kafka.consumer.KafkaConsumerSpecSetup.{ConsumerDefaults, MockKafkaStream}
import com.socrata.balboa.kafka.test.util.ConsumerTestUtil
import com.socrata.balboa.metrics.data.DataStore
import com.socrata.balboa.metrics.impl.JsonMessage
import kafka.message.MessageAndMetadata
import org.mockito.Mockito
import org.scalatest.WordSpec
import org.scalatest.mock.MockitoSugar


object BalboaConsumeSpecSetup extends MockitoSugar {

  trait MockDataStore {
    val mDS = mock[DataStore]
  }

  trait TestBalboaConsumer extends MockKafkaStream with MockDataStore with ConsumerDefaults {
    val balboaConsumer: BalboaConsumer = new BalboaConsumer(mStream, waitTime, mDS)
  }

}

/**
 * Unit and Integration test for Balboa Consumer
 */
class BalboaConsumerSpec extends WordSpec {

  "A BalboaConsumer" should {

    "persist 1 entry to the data store" in new TestBalboaConsumer {
      Mockito.when(mIterator.hasNext()).thenReturn(true, false)
      val json: JsonMessage = ConsumerTestUtil.testJSONMetric("test_metric", 1)
      val mm = ConsumerTestUtil.message(json)
      Mockito.when(mIterator.next()).thenReturn(mm)
      balboaConsumer.run()
      Mockito.verify(mDS).persist(json.getEntityId, json.getTimestamp, json.getMetrics)
    }

    "persist multiple entries to the data store" in new TestBalboaConsumer {
      Mockito.when(mIterator.hasNext()).thenReturn(true, true, true, true, true, false)
      var jsons: List[JsonMessage] = List[JsonMessage]()
      var mms = List[MessageAndMetadata[Array[Byte], Array[Byte]]]()
      for (i <- 1 to 5) {
        val json = ConsumerTestUtil.testJSONMetric(s"test_metric_$i", i)
        jsons = jsons :+ json
        mms = mms :+ ConsumerTestUtil.message(json)
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
