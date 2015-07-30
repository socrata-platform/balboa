package com.socrata.balboa.service.kafka.consumer

import java.io.IOException

import com.socrata.balboa.common.logging.BalboaLogging
import com.socrata.balboa.metrics.data.BalboaFastFailCheck

trait PersistentKafkaConsumerReadiness extends KafkaConsumerReadiness {

  // Failfast check for assessing consumption readiness
  val fastFailCheck: BalboaFastFailCheck = BalboaFastFailCheck.getInstance()

  /**
   * @return Whether or not this consumer is ready or not.
   */
  override def ready: Boolean = fastFailCheck.proceed
}

/**
 * A [[KafkaConsumerComponent]] that allows messages to handle failure prone persist method calls.  An example of this
 *  is writing messages to a data store or data base.  Because there is a dependency on a third part system this consumer
 *  attempts to handle failures by backing off retries.  This is to handle the case of intermediate brief system flops.
 */
trait PersistentKafkaConsumerComponent[K,M] extends KafkaConsumerComponent[K,M] {

  trait PersistentKafkaConsumer extends KafkaConsumer with BalboaLogging {
    self: KafkaConsumerStreamProvider[K,M] with PersistentKafkaConsumerReadiness =>

    /**
     * Attempt to persist a message.
     *
     * @param message entity to persist.
     * @throws IOException if there was a recoverable error.
     */
    @throws[IOException]
    protected def persist(key: K, message: M): Unit

    /**
     * Call to consume the Key-Message pair.
     *
     * See [[ConsumerLike.consume()]].
     */
    @annotation.tailrec
    override final def consume(key: K, message: M): Option[String] = {
      // Note: Scala compiler will unravel this tail recursion into a while loop,
      // As an effect, Having the node or killing the service will potentially result in a message loss
      try {
        persist(key, message)
        fastFailCheck.markSuccess()
        None
      } catch {
        case e: IOException =>
          // Wrap the exception to provide a more descriptive message.
          fastFailCheck.markFailure(new IOException(s"Error persisting Kafka Key-Message: $key-$message.", e))
          waitUntilReady()
          consume(key, message)
        case e: Exception =>
          logger.error(s"Unable to persist Kafka Key-Message: $key-$message.", e)
          val errorMessage = e.getMessage
          Some(s"Unable to persist Kafka Key-Message: $key-$message..  Reason: $errorMessage")
      }
    }
  }
}
