package com.socrata.balboa.common.kafka

/**
 * Kafka environment settings for Staging and RC.
 */
object TestEnvironment {

  /**
   * Currently have a partition number of 4
   */
  val NUM_PARTITIONS: Int = 8

  /**
   * Current replication factor of three so that we can have at most up to two nodes down at any time.
   */
  val REPLICATION_FACTOR: Int = 3

  /**
   * The current cluster size of our environment in staging and rc
   */
  val SERVER_COUNT: Int = 4

}
