package com.socrata.balboa.common.kafka.consumer

import com.socrata.balboa.service.consumer.ConsumerComponent
import kafka.consumer.{ConsumerIterator, KafkaStream}
import kafka.message.MessageAndMetadata

/**
 * This traits defines the requirement for identifying the stream that a Kafka Consumer should use.
 *
 * @tparam K Kafka Key type
 * @tparam M Kafka Message type
 */
trait KafkaConsumerStreamProvider[K,M] {

  /**
   * The Stream to consume input from.
   */
  val stream: KafkaStream[K,M]
  
}

/**
 * Trait that defines when a consumer is ready and how long it is willing to
 * wait until it is ready.
 */
trait KafkaConsumerReadiness {

  /**
   * How long in ms this Consumer is willing to wait until it is ready again.
   */
  val waitTime: Long

  /**
   * @return Whether or not this consumer is ready or not.
   */
  def ready: Boolean

}

/**
 * Component that embodies a single threaded consumer.  This Consumer
 *
 * @tparam K Kafka Key Type
 * @tparam M Kafka Message Type
 */
trait KafkaConsumerComponent[K,M] extends ConsumerComponent[(K,M)] {

  trait KafkaConsumer extends ConsumerLike {
    self: KafkaConsumerStreamProvider[K,M] with KafkaConsumerReadiness =>

    /**
     * See [[ConsumerLike.run()]]
     */
    override def run(): Unit = {
      val it: ConsumerIterator[K, M] = stream.iterator()
      // If the consumer is configured to not time out, then this loop will iterate
      while (it.hasNext()) {
        waitUntilReady()
        // At this point we take control of the message away from Kafka.
        val m: MessageAndMetadata[K,M] = it.next()
        consume((m.key(), m.message()))
      }
    }

    /**
     * Signals this consumer to attempt complete the current job and stop consuming any more messages.
     *
     * Reference: [[ConsumerLike.stop()]]
     */
    override def stop(): Unit = {
      // TODO Handle shutdown.
    }

    /**
     * Call to consume the Key-Message pair.
     *
     * See [[ConsumerLike.consume()]].
     */
    override def consume(item: (K,M)): Option[String]

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
}
