package com.socrata.metrics.producer

import java.util.Properties

import com.socrata.balboa.metrics.util.AddressAndPort
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.Encoder

/**
 * A wrapper around Kafka [[Producer]].  This Producer is restricted to exactly one Kafka Topic.  This
 * producers responsibility is to provide many of the default properties for Kafka [[Producer]].  To see the
 * current default settings reference [[producerDefaultProps]].  You can override any of the default
 * properties with passing in your own [[Properties]].  The only
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
                                                                                      brokers: List[AddressAndPort],
                                                                                      properties: Option[Properties] = None)
  extends AutoCloseable {

  // See: Kafka Producer Config: producer.type
  // Call will block the current thread
  private val PRODUCER_TYPE = "sync"

  // See: Kafka Producer Config: request.required.acks
  // Full syncronization.
  private val REQUEST_REQUIRED_ACKS = (-1).toString

  // Dynamically configure encoder by name.
  val producer = {
    val p = producerDefaultProps
    properties match {
      case Some(other) => p.putAll(other)
      case None => // NOOP
    }
    p.setProperty("serializer.class", manifest[ME].runtimeClass.getName)
    p.setProperty("key.serializer.class", manifest[KE].runtimeClass.getName)

    brokers match {
      case Nil =>
        throw new IllegalArgumentException("Cannot create a Generic Kafka Producer with an empty broker list.")
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
  override def close(): Unit = producer.close()

  /**
   * While this returns the base configuration for producers, the returned property
   *  instance still require metadata.broker.list, serializer.class, key.serializer.class.
   *
   * @return Default Properties for producers.
   */
  protected def producerDefaultProps: Properties = {
    val p = new Properties()
    p.setProperty("request.required.acks", REQUEST_REQUIRED_ACKS)
    p.setProperty("producer.type", PRODUCER_TYPE)
    // BalboaKafka Developers: Add more Socrata default producer configs here...
    // These configurations should coincide with how Kafka Brokers are configured.
    p
  }

}
