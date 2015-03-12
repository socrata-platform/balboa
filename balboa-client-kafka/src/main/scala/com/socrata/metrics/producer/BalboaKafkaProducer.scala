package com.socrata.metrics.producer

import java.util.Properties

import com.socrata.balboa.kafka.Constants
import com.socrata.balboa.kafka.util.AddressAndPort
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.Encoder

/**
 * A wrapper around Kafka [[Producer]].  This Producer is restricted to exactly one Kafka Topic.  The producer is
 * currently this producer is configured to send messages Syncronously (See: [[Constants.producerDefaultProps]]).
 *
 * @tparam K Kafka Key type.  Can be anything as long as the Encoder is appropiately configured
 * @tparam M Kafka Message type.  Can be anything as long as the Encoder is appropiately configured
 */
trait BalboaKafkaProducer[K,M] {

  val producer: Producer[K,M]
  val topic: String

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
  def close() = producer.close()

}

// Only One Constructor
sealed case class ::[K,M](topic: String, producer: Producer[K,M]) extends BalboaKafkaProducer[K,M]

object BalboaKafkaProducer {

  /**
   * Creates a Kafka Producer with a list of brokers and a specific topic.
   *
   * @param topic Kafka topic to publish to.
   * @param brokers A list of Kafka brokers.  If none is provided then consul will attempted to
   *                be used to discover broker services.
   * @param properties Properties to use that overwrite defaults found [[Constants.producerDefaultProps]].  It does not
   *                   overwrite the key or message encoders.  Those will always be defined in the type parameter.
   * @tparam K Key type
   * @tparam M Message type
   * @tparam KE Encoder type that will be used to encode kafka message "keys"
   * @tparam ME Encoder type that will be used to encode kafka "messages"
   * @return Left(error), or Right(KafkaProducer)
   */
  def cons[K,M, KE <: Encoder[K]: Manifest, ME <: Encoder[M]: Manifest](topic: String,
                                                                        brokers: List[AddressAndPort] = List.empty,
                                                                        properties: Option[Properties] = None):
  Either[String, BalboaKafkaProducer[K,M]] = {
    if (topic == null || topic.trim.isEmpty)
      return Left(s"Illegal topic: $topic")

    // Dynamically configure encoder by name.
    val p = Constants.producerDefaultProps
    properties match {
      case Some(other) => p.putAll(other)
      case None => // NOOP
    }
    p.setProperty("serializer.class", manifest[ME].runtimeClass.getName)
    p.setProperty("key.serializer.class", manifest[KE].runtimeClass.getName)

    // Either use consul or use configuration.
    brokers match {
      case Nil =>
        // TODO Implement
        Left("Consul not yet implemented, Please use explicit list of brokers.")
      case l =>
        p.setProperty("metadata.broker.list", l.map(aap => aap.toString).mkString(","))
        Right(::(topic.trim, new Producer(new ProducerConfig(p))))
    }
  }

}
