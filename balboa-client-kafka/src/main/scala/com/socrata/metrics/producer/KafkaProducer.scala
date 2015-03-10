package com.socrata.metrics.producer

import kafka.producer.{KeyedMessage, Producer}
import org.apache.commons.logging.{Log, LogFactory}

/**
 * Generic Kafka Producer.  Consumers will be continuously running service threads.  Therefore,
 * this implementation allows the ability to wait for any dependent services to be ready.  This is
 * done by enforcing the implementation of the ready method (@see #ready).  The consumer will wait
 * to convert and process a message once the consumer is ready.  If the consumer is not ready then
 * it will wait a designated amount of time.
 *
 * @tparam A Message type
 */
abstract class KafkaProducer[A](topic: String, kp: Producer[String, A]) {

  protected val log: Log = LogFactory.getLog(classOf[KafkaProducer[A]])

  /**
   * Sends a message thr
   *
   * @param key Key to associate with this message.  Can be null
   * @param message Message to send.
   */
  def send(key: String, message: A): Unit = {
    kp.send(new KeyedMessage(topic, key, message))
  }
}
