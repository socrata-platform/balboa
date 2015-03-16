package com.socrata.balboa.service.consumer

/**
 * Trait that defines the underlying requirement for consuming elements of parameterized type.
 *
 * @tparam A Type of element to consume.
 */
trait ConsumerComponent[A] {
  type Consumer <: ConsumerLike

  /**
   * Underlying consumer that is meant to continuous consume messages until shutdown, stopped, or the method of consumption
   *  does not block and the consumer completes.
   */
  trait ConsumerLike extends Runnable {

    /**
     * Called when the client wishes to gracefully shutdown any currently running consumptions or halts the consumer from
     * waiting any further for following requests.
     */
    def stop(): Unit

    /**
     * Ingests the element.  Returns an error if ingestion fails in the form of an optional description.
     *
     * @param item The item to consume.
     * @return Some(error) in case of a failed consumption, or [[None]] in case of success!
     */
    def consume(item: A): Option[String]

  }

}