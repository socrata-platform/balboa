package com.socrata.balboa.common.kafka.codec

import BalboaMessageCodecSpecSetup.Codec
import com.socrata.balboa.common.kafka.util.TestMetricsStuff
import com.socrata.balboa.common.kafka.util.TestMetricsStuff.TestMessages
import TestMetricsStuff.TestMessages
import org.scalatest.WordSpec

/**
 * Setup Helper code.
 */
object BalboaMessageCodecSpecSetup {

  trait Codec {
    val balboaMessageCodec = new BalboaMessageCodec()
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
