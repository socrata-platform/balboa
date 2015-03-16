package com.socrata.balboa.common.kafka.test.integration

import java.util.Properties

import com.socrata.balboa.common.kafka.codec.{BalboaMessageCodec, StringCodec}
import com.socrata.balboa.common.kafka.util.AddressAndPort
import com.socrata.balboa.metrics.Message
import com.socrata.integration.kafka.util.BalboaClientTestHarness
import com.socrata.metrics.producer.BalboaKafkaProducer

/**
 * ScalaTest Harness base for testing Kafka Consumption Services.  This harness uses
 */
trait BalboaServiceTestHarness[K, M] extends BalboaClientTestHarness[K,M] {

  val producerCount: Int
  val consumerCount: Int
  val serverCount: Int
  val topic: String

  /**
   * Because we are testing consuming services we do not need any default
   * consumers.
   */
  override val consumerGroupCount: Int = 0

//  TODO
//  def genConsumerGroup(topic: String, consumerConfig: ConsumerConfig): KafkaConsumerGroup[K,M]


}

trait BalboaMessageServiceTestHarness extends BalboaClientTestHarness[String, Message] {

  /**
   * See [[BalboaClientTestHarness.genProducer()]]
   */
  override protected def genProducer(topic: String,
                                     brokers: List[AddressAndPort],
                                     properties: Option[Properties]): BalboaKafkaProducer[String, Message] =
    BalboaKafkaProducer.cons[String, Message, StringCodec, BalboaMessageCodec](
      topic, AddressAndPort.parse(this.bootstrapUrl), Some(producerConfig)) match {
      case Left(error) => throw new IllegalStateException("Unable to initialize producers: " + error)
      case Right(p) => p
    }
}
