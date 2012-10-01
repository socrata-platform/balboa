package com.socrata.balboa.metrics.data.impl

import com.socrata.balboa.metrics.data.{Period, DataStore}
import com.socrata.balboa.metrics.Metrics
import java.{ util => ju}
import scala.collection.JavaConverters._
import org.apache.commons.logging.{LogFactory, Log}

/**
 * All the bad ideas go here. Things which are not clear from the API, or which seem odd.
 */
class BadIdeasDataStore(child:DataStore) extends DataStoreImpl {
  private var log: Log = LogFactory.getLog(classOf[BadIdeasDataStore])

  def entities(pattern: String) = {
    log.error("Getting all the entities from balboa is a slow, dangerous thing. Please do not do this regularly.")
    child.entities(pattern)
  }

  def entities() = {
    log.error("Getting all the entities from balboa is a slow, dangerous thing. Please do not do this regularly.")
    child.entities()
  }

  def slices(entityId: String, period: Period, start: ju.Date, end: ju.Date) = child.slices(entityId, period, start, end)
  def find(entityId: String, period: Period, date: ju.Date) = child.find(entityId, period, date)
  def find(entityId: String, period: Period, start: ju.Date, end: ju.Date) = child.find(entityId, period, start, end)
  def find(entityId: String, start: ju.Date, end: ju.Date) = child.find(entityId, start, end)
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
