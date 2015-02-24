package com.socrata.balboa.kafka.consumer

import java.io.IOException

import com.socrata.balboa.metrics.Message
import com.socrata.balboa.metrics.data.{BalboaFastFailCheck, DataStore}
import com.socrata.balboa.metrics.impl.JsonMessage
import kafka.consumer.{ConsumerIterator, KafkaStream}
import kafka.message.MessageAndMetadata
import org.apache.commons.logging.{LogFactory, Log}

/**
 * Generic Kafka Consumer.  Consumers will be continuously running service threads.  Therefore,
 * this implementation allows the ability to wait for any dependent services to be ready.  This is
 * done by enforcing the implementation of the ready method (@see #ready).  The consumer will wait
 * to convert and process a message once the consumer is ready.  If the consumer is not ready then
 * it will wait a designated amount of time.
 *
 * @param stream Kafka Stream for ingesting data
 * @param waitTime The amount of time to wait if the consumers dependency are not ready (ms).
 * @tparam A Message type
 */
abstract class KafkaConsumer[A](stream: KafkaStream[Array[Byte],Array[Byte]], waitTime: Long)
  extends Runnable {
  // Notes:
  // 1. We used abstract classes because parameters are required
  // 2. Variance: ???.  Currently going with the safe choice of invariance.

  protected val log: Log = LogFactory.getLog(classOf[KafkaConsumer[A]])

  override def run(): Unit = {
    val it: ConsumerIterator[Array[Byte], Array[Byte]] = stream.iterator()
    while (it.hasNext()) {
      // Block if necessary.
      waitUntilReady()
      // At this point we take control of the message away from Kafka.
      onMessageAndMetaData(it.next())
    }
  }

  /**
   * Converts a Kafka key, message pair into an instance of type "A".
   * <b>
   *  For a reference to Kafka Messages see: {@link http://kafka.apache.org/documentation.html#messages}
   *
   * @param key Kafka message key
   * @param message Kafka message content
   * @return A converted key-message pair or a reason for the failure.
   */
  protected def convert(key: Array[Byte], message: Array[Byte]): Either[String, A]

  /**
   * Receiver for incoming messages.  This method should only be overridden if
   * you have a any special decoding needs.  If you
   * This method calls onMessage(JsonMessage)
   *
   * @param mm Received Kafka Message
   */
  private def onMessageAndMetaData(mm: MessageAndMetadata[Array[Byte],Array[Byte]]): Unit = {
    if (mm == null) {
      log.warn(s"Message and Metadata from Kafka Stream $stream is null. Skipping")
      return
    }
    if (mm.message() == null) {
      log.warn(s"Message from Kafka Stream $stream is null. Skipping")
      return
    }
    convert(mm.key(), mm.message()) match {
      case Left(error) =>
        log.error(s"Unable to Convert Kafka MessageAndMetaData, Reason: $error")
      case Right(m) => onMessage(m)
    }
  }

  /**
   * Function that handles the reception of new Messages
   */
  protected def onMessage(message: A): Unit

  /**
   * @return Whether or not this consumer is ready to consume the next message.
   */
  protected def ready: Boolean

  /**
   * Waits on the current thread for this consumer to be ready.
   */
  protected final def waitUntilReady() = {
    while (!ready) {
      // TODO Check if the kill signal has been dispatched. If so, kill the current thread.
      // Potentially loosing a message
      Thread.sleep(waitTime)
    }
  }

}

/**
 * Consumer that provides the interface to consume messages from a kafka stream and persist
 * them into a sink defined by the subclass implementation.
 *
 * @param stream Kafka Stream to read from.
 * @param waitTime Time in milliseconds for a thread to wait.
 */
abstract class PersistentConsumer[A](stream: KafkaStream[Array[Byte],Array[Byte]],
                                     waitTime: Long) extends KafkaConsumer[A](stream, waitTime) {

  // Balboa fail fast check.
  val fastFailCheck = BalboaFastFailCheck.getInstance()

  /**
   * Attempt to persist new messages where ever the subclass defines
   */
  @annotation.tailrec
  final override protected def onMessage(message: A): Unit = {
    // Note: Scala compiler will unravel this tail recursion into a while loop,
    // As an effect, Having the node or killing the service will potentially result in message loss
    try {
      persist(message)
      fastFailCheck.markSuccess()
    } catch {
      case e: IOException =>
        // Wrap the exception to provide a more descriptive message.
        fastFailCheck.markFailure(new IOException(s"Error persisting message: $message", e))
        waitUntilReady()
        onMessage(message)
      case e: Exception =>
        // TODO  non recoverable exception
        throw e
    }
  }

  /**
   * Attempt to persist a message
   *
   * @param message entity to persist
   */
  @throws[IOException]
  protected def persist(message: A): Unit

  /**
   * Check Data Store against Fail Fast instance.
   *
   * @return Whether or not this consumer is ready to consume the next message.
   */
  final override protected def ready: Boolean = fastFailCheck.proceed()

}
