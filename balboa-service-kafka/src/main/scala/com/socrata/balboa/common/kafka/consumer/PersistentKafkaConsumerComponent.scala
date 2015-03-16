package com.socrata.balboa.common.kafka.consumer

import java.io.IOException

import com.socrata.balboa.metrics.data.BalboaFastFailCheck
import org.apache.commons.logging.LogFactory

trait PersistentKafkaExternalComponents[K,M] extends KafkaConsumerStreamProvider[K,M] {

  // Add members as necessary as necessary.

}

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
    self: PersistentKafkaExternalComponents[K,M] with PersistentKafkaConsumerReadiness =>

    private val Log = LogFactory.getLog(classOf[PersistentKafkaConsumer])

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
    override final def consume(item: (K, M)): Option[String] = {
      // Note: Scala compiler will unravel this tail recursion into a while loop,
      // As an effect, Having the node or killing the service will potentially result in a message loss
      try {
        persist(item._1, item._2)
        fastFailCheck.markSuccess()
        None
      } catch {
        case e: IOException =>
          // Wrap the exception to provide a more descriptive message.
          fastFailCheck.markFailure(new IOException(s"Error persisting message: $item", e))
          waitUntilReady()
          consume(item)
        case e: Exception =>
          Log.error(s"Unable to persist message: $item.", e)
          val errorMessage = e.getMessage
          Some(s"Unable to persist message: $item.  Reason: $errorMessage")
      }
    }
  }

}
