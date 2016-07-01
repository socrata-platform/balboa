package com.socrata.balboa.service.kafka.consumer

// scalastyle: off

import java.io.IOException

import com.socrata.balboa.metrics.data.BalboaFastFailCheck
import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

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

  trait PersistentKafkaConsumer extends KafkaConsumer {
    self: KafkaConsumerStreamProvider[K,M] with PersistentKafkaConsumerReadiness =>

    private val Log = Logger(LoggerFactory getLogger this.getClass)

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
    override final def consume(key: K, message: M, attempts: Int = 0): Option[String] = {
      // Note: Scala compiler will unravel this tail recursion into a while loop,
      // As an effect, Having the node or killing the service will potentially result in a message loss
      
      if(attempts >= retries) {
        Log.error("Exceeded allowed number of retries")
        throw new IOException("Exceeded allowed number of retries")
      }

      try {
        persist(key, message)
        fastFailCheck.markSuccess()
        None
      } catch {
        case e: IOException =>
          // Wrap the exception to provide a more descriptive message.
          fastFailCheck.markFailure(new IOException(s"Error persisting Kafka Key-Message: $key-$message.", e))
          waitUntilReady()
          consume(key, message, attempts + 1)
        case e: Exception =>
          Log.error(s"Unable to persist Kafka Key-Message: $key-$message.", e)
          val errorMessage = e.getMessage
          Some(s"Unable to persist Kafka Key-Message: $key-$message..  Reason: $errorMessage")
      }
    }
  }
}
