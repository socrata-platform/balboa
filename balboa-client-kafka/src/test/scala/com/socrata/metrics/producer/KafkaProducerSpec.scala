package com.socrata.metrics.producer

import com.socrata.balboa.kafka.codec.StringCodec
import com.socrata.balboa.metrics.Message
import com.socrata.integration.kafka.util.{StringClientTestHarness, BalboaClientTestHarness}
import kafka.consumer.Consumer
import kafka.server.KafkaConfig
import kafka.utils.{TestZKUtils, TestUtils}
import org.scalatest.WordSpec
import kafka.api

/**
 * Sets up a single Producer, Consumer, and Broker
 */
trait StringMessageSetup extends StringClientTestHarness {
  override val producerCount: Int = 1
  override val serverCount: Int = 1
  override val consumerCount: Int = 1
  override val topic: String = "string_topic"
}

/**
 *
 */
class KafkaProducerSpec extends WordSpec {

  "A KafkaProducer" should {

    "not be created in case of malformed input" in {
      val result = BalboaKafkaProducer.cons[String, String, StringCodec, StringCodec](null)
      assert(result.isLeft, "Null Topics should always fail.")

      val result2 = BalboaKafkaProducer.cons[String, String, StringCodec, StringCodec](" ")
      assert(result2.isLeft, "Topic names with space should always fail.")
    }

    "not be created" in {}

    "should be able to send a message " in new StringMessageSetup {
//      producers(0).send("Hello World")
    }
  }

}
