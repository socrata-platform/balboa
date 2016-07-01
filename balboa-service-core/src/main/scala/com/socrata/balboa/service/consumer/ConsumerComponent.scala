package com.socrata.balboa.service.consumer

import java.util.concurrent.Callable

/**
 * Trait that defines the underlying requirement for consuming elements of parameterized type.
 *
 * @tparam A Type of element to consume.
 */
trait ConsumerComponent[A] {
  type Consumer <: ConsumerLike

  /**
   * Underlying consumer that is meant to continuous consume messages until shutdown, stopped, or the method of
   * consumption does not block and the consumer completes.
   */
  trait ConsumerLike extends Callable[Option[Exception]] with AutoCloseable {

    /**
     * Start the consumption process.  This is a synchronous call.
     *
     * @return Some(exception) if something unexpected happened while starting up.
     */
    def start(): Option[Exception]

    /**
     * Called when the client wishes to gracefully shutdown any currently running consumptions or halts the consumer
     * from waiting any further for following requests.
     *
     * @return Some(exception) if something unexpected happened while shutting down.
     */
    def stop(): Option[Exception]

    /**
     * Ingests the element.  Returns an error if ingestion fails in the form of an optional description.
     *
     * @param item The item to consume.
     * @return Some(error) in case of a failed consumption, or [[None]] in case of success!
     */
    def consume(item: A): Option[String]

    /**
     * Starts the consumption process.  If you want to start the consumption process explicitly this is the same
     * as effectively calling [[start()]]
     */
    override final def call(): Option[Exception] = this.start()

    /**
     * Auto closes by call [[stop()]].
     */
    override final def close(): Unit = this.stop()
  }
}
