package com.socrata.balboa.service.consumer

import java.util.concurrent._

/**
 * Abstract General Consumer Group for consuming elements of type A.  This class is allows the ability to branch
 *  and create different kinds of consumption processes. IE. Database/DataStore Consumer, Consol Output, forwarding
 *  service to another stream.
 */
trait ConsumerGroupComponent[A] extends Callable[List[Future[Option[Exception]]]] with AutoCloseable {
  self: ConsumerComponent[A] =>

  /**
   * Creates the base executor
   */
  lazy val executor: ExecutorService = Executors.newFixedThreadPool(consumers.size)

  /**
   * @return A list of Consumers that belong exclusively to this group.
   */
  def consumers: List[ConsumerLike]

  /**
   * Shutdowns the current running threads.
   */
  def stop(): Option[Exception] = try {
    executor.shutdown()
    None
  } catch {
    case e:Exception => Some(e)
  }

  /**
   * Executes all the consumers in a fixed size thread pool.  A list of future task is returned to represent consumer
   * completion.  Having future task allow the calling entity to identify when a consumer(s) complete their consumption
   *  task.
   *
   *  <br>
   *    IMPORTANT NOTE: If you set your consumer to wait infinitely then call [[FutureTask.get]] will block on the
   *    the current thread infinitely.
   *
   * @return A list of [[Future]]s that correspond for each Consumer.
   */
  def start(): List[Future[Option[Exception]]] = consumers.map(c => executor.submit(c))

  /**
   * Starts all the consumers.
   *
   * @return
   */
  override final def call(): List[Future[Option[Exception]]] = start()

  /**
   * AutoCloses by calling [[stop()]].  Place all shutdown and clean up processes overriding
   * [[stop()]] but remember to call `super.stop()`
   */
  override final def close(): Unit = stop()
}
