package com.socrata.balboa.service.consumer

import java.util.concurrent.{Executors, ExecutorService}

/**
 *
 */
trait ConsumerGroupComponent[A] {
  self: ConsumerComponent[A] =>

  /**
   * Number of Consumers to have in this group.  By convention, for Kafka we use the same number of threads as there
   * are partitions for a given topic.
   */
  val numConsumers: Int = consumers.size

  /**
   * Creates the base executor
   */
  val executor: ExecutorService = Executors.newFixedThreadPool(numConsumers)

  /**
   * @return A list of Kafka Consumers that belong exclusively to this group.
   */
  def consumers: List[ConsumerLike]

  /**
   * Starts all the individual consumers within a fixed size threadpool.
   */
  def run() = consumers.foreach(c => executor.submit(c))

}
