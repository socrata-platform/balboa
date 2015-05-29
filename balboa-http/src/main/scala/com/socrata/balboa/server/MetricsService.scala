package com.socrata.balboa.server

import java.util.Date

import com.socrata.balboa.metrics.{Timeslice, Metrics}
import com.socrata.balboa.metrics.data.{DataStoreFactory, Period}

import scala.collection.JavaConversions._

/**
 * Service layer that provides Metrics related functionality.
 */
object MetricsService {

  /*
  Development Notes:

  Class was created to abstract HTTP and actualy service functionality.  This is currrently required as apart of using
  Socrata Http.
   */

  // Reference the data store from the factory method.
  val ds = DataStoreFactory.get()

  /**
   * Given a specific entity id, target period, and time range find all the metrics for each period ranging from start time to end time.
   *
   * @param entityId The entity ID to obtain metrics for.
   * @param period The interval to collect metrics at.
   * @param start The earliest date to obtain metrics for..
   * @param end The latest date to obtain metrics for.
   * @return An iterable collection of Timeslices ranged by the period with respective metrics for that entity ID.
   */
  def series(entityId: String, period: Period, start: Date, end: Date): Iterator[Timeslice] = ds.slices(entityId, period, start, end)

}
