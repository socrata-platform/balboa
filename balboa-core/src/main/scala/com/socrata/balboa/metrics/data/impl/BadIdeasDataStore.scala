package com.socrata.balboa.metrics.data.impl

import com.socrata.balboa.metrics.data.{Period, DataStore}
import com.socrata.balboa.metrics.Metrics
import java.util.Date
import scala.collection.JavaConverters._

/**
 * All the bad ideas go here. Things which are not clear from the API, or which seem odd.
 */
class BadIdeasDataStore(child:DataStore) extends DataStoreImpl {
  def entities(pattern: String) = child.entities(pattern)
  def entities() = child.entities()
  def slices(entityId: String, period: Period, start: Date, end: Date) = child.slices(entityId, period, start, end)
  def find(entityId: String, period: Period, date: Date) = child.find(entityId, period, date)
  def find(entityId: String, period: Period, start: Date, end: Date) = child.find(entityId, period, start, end)
  def find(entityId: String, start: Date, end: Date) = child.find(entityId, start, end)
  def persist(entityId: String, timestamp: Long, metrics: Metrics) = {
    if (entityId.startsWith("__") && entityId.endsWith("__"))
    {
      throw new IllegalArgumentException("Unable to persist entities " +
        "that start and end with two underscores '__'. These " +
        "entities are reserved for meta data.");
    }

    metrics.asScala.foreach {
      case (key, value) => {
        if (key.startsWith("__") && key.endsWith("__")) {
          throw new IllegalArgumentException("Unable to persist metrics " +
            "that start and end with two underscores '__'. These " +
            "entities are reserved for meta data.")
        }
      }
    }
    child.persist(entityId, timestamp, metrics)
  }
}
