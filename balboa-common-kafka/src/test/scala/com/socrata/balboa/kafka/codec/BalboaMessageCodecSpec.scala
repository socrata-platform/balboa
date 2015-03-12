package com.socrata.balboa.kafka.codec

import com.socrata.balboa.kafka.codec.BalboaMessageCodecSpecSetup.{TestMessages, Codec, TestMetrics}
import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.{Metric, Metrics}
import com.socrata.balboa.metrics.impl.JsonMessage
import org.scalatest.WordSpec

/**
 * Setup Helper code.
 */
object BalboaMessageCodecSpecSetup {

  trait Codec {
    val balboaMessageCodec = new BalboaMessageCodec()
  }

  /**
   * Metrics
   */
  trait TestMetrics {
    val emptyMetrics = new Metrics()
    val oneElemMetrics = new Metrics()
    oneElemMetrics.put("cats", metric(1))
    val manyElemMetrics = new Metrics()
    manyElemMetrics.put("cats", metric(1))
    manyElemMetrics.put("giraffes", metric(2))
    manyElemMetrics.put("dogs", metric(3))
    manyElemMetrics.put("penguins", metric(4))
    manyElemMetrics.put("monkeys", metric(5))
    manyElemMetrics.put("rhinos", metric(6))
  }

  /**
   * Precomposed messages.
   */
  trait TestMessages extends TestMetrics {
    val emptyMessage = new JsonMessage()
    emptyMessage.setEntityId("empty")
    emptyMessage.setTimestamp(1)
    emptyMessage.setMetrics(emptyMetrics)

    val oneElemMessage = new JsonMessage()
    oneElemMessage.setEntityId("oneElem")
    oneElemMessage.setTimestamp(1)
    oneElemMessage.setMetrics(oneElemMetrics)

    val manyElemMessage = new JsonMessage()
    manyElemMessage.setEntityId("many")
    manyElemMessage.setTimestamp(1)
    manyElemMessage.setMetrics(manyElemMetrics)
  }

  def metric(value: Number, t: RecordType = RecordType.AGGREGATE): Metric = {
    val m = new Metric()
    m.setType(t)
    m.setValue(value)
    m
  }
}

/**
 * BalboaMessageCodec Unit test.
 */
class BalboaMessageCodecSpec extends WordSpec {

  "A Balboa Message Codec" should {

    "be reflexive with empty metric set" in new TestMessages with Codec {
      val bytes: Array[Byte] = balboaMessageCodec.toBytes(emptyMessage)
      val newMessage = balboaMessageCodec.fromBytes(bytes)
      assert(newMessage.toString.equals(emptyMessage.toString), "Not equal after transformation")
    }

    "be reflexive with non empty set" in new TestMessages with Codec {
      val bytes: Array[Byte] = balboaMessageCodec.toBytes(oneElemMessage)
      val newMessage = balboaMessageCodec.fromBytes(bytes)
      assert(newMessage.toString.equals(oneElemMessage.toString), "Not equal after transformation")
    }

    "be reflexive with set with many elements" in new TestMessages with Codec {
      val bytes: Array[Byte] = balboaMessageCodec.toBytes(manyElemMessage)
      val newMessage = balboaMessageCodec.fromBytes(bytes)
      assert(newMessage.toString.equals(manyElemMessage.toString), "Not equal after transformation")
    }

  }

}
