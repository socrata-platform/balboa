package com.socrata.metrics.producer

import java.util.Properties
import com.socrata.balboa.common.kafka.Constants
import com.socrata.balboa.common.kafka.util.AddressAndPort
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.Encoder

/**
 * A wrapper around Kafka [[Producer]].  This Producer is restricted to exactly one Kafka Topic.  This
 * producers responsibility is to provide many of the default properties for Kafka [[Producer]].  To see the
 * current default settings reference [[Constants.producerDefaultProps]].  You can override any of the default
 * properties with passing in your own properties.
 *
 * @param topic Kafka topic to produce to.
 * @param brokers Kafka Broker metadata list.
 * @param properties Producer properties.
 * @tparam K Kafka "Key" type
 * @tparam M Kafka "Message" type
 * @tparam KE Encoder for K. Must have a constructor argument signature of `properties: VerifiableProperties = null`
 * @tparam ME Encoder for M. Must have a constructor argument signature of `properties: VerifiableProperties = null`
 */
class GenericKafkaProducer[K,M,KE <: Encoder[K]: Manifest,ME <: Encoder[M]: Manifest](topic: String,
                                                                                      brokers: List[AddressAndPort] = List.empty,
                                                                                      properties: Option[Properties] = None)
  extends AutoCloseable {

  // Dynamically configure encoder by name.
  val producer = {
    val p = Constants.producerDefaultProps
    properties match {
      case Some(other) => p.putAll(other)
      case None => // NOOP
    }
    p.setProperty("serializer.class", manifest[ME].runtimeClass.getName)
    p.setProperty("key.serializer.class", manifest[KE].runtimeClass.getName)

    brokers match {
      case Nil =>
      // TODO Implement Consul Service discovery.
      case l =>
        p.setProperty("metadata.broker.list", l.map(aap => aap.toString).mkString(","))
    }
    new Producer[K,M](new ProducerConfig(p))
  }

  /**
   * Sends a message via Kafka Producer.  Identical to sending a message with a null key.
   *
   * @param message Message to send.
   */
  def send(message: M): Unit = {
    producer.send(new KeyedMessage[K,M](topic, message))
  }

  /**
   * Sends a message with a correlated key.  The key may be null.  If the key is not null then
   * it is used by whatever partitioner is configured.
   */
  def send(key: K, message: M): Unit = {
    producer.send(new KeyedMessage[K,M](topic, key, message))
  }

  /**
   * Closes the Kafka Producer.
   * <br>
   *  See: [[Producer.close]]
   */
  override def close() = producer.close()

}
