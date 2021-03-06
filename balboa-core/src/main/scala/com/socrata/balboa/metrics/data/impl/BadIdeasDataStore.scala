package com.socrata.balboa.metrics.data.impl

import java.{util => ju}

import com.socrata.balboa.metrics.{Metrics, Timeslice}
import com.socrata.balboa.metrics.data.{DataStore, Period}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._

/**
 * All the bad ideas go here. Things which are not clear from the API, or which seem odd.
 */
class BadIdeasDataStore(child:DataStore) extends DataStoreImpl with StrictLogging {
  val DoubleUnderscore = "__"

  @throws(classOf[Exception])
  def checkHealth(): Unit = {
    child.checkHealth()
  }

  def entities(pattern: String): Iterator[String] = {
    logger error "Getting all the entities from balboa is a slow, dangerous thing. Please do not do this regularly."
    child.entities(pattern)
  }

  def entities(): Iterator[String] = {
    logger error "Getting all the entities from balboa is a slow, dangerous thing. Please do not do this regularly."
    child.entities()
  }

  def slices(entityId: String, period: Period, start: ju.Date, end: ju.Date): Iterator[Timeslice] =
    child.slices(entityId, period, start, end)

  def find(entityId: String, period: Period, date: ju.Date): Iterator[Metrics] =
    child.find(entityId, period, date)

  def find(entityId: String, period: Period, start: ju.Date, end: ju.Date): Iterator[Metrics] =
    child.find(entityId, period, start, end)

  def find(entityId: String, start: ju.Date, end: ju.Date): Iterator[Metrics] =
    child.find(entityId, start, end)

  def persist(entityId: String, timestamp: Long, metrics: Metrics): Unit = {
    if (entityId.startsWith(DoubleUnderscore) && entityId.endsWith(DoubleUnderscore))
    {
      throw new IllegalArgumentException("Unable to persist entities " +
        s"that start and end with two underscores '$DoubleUnderscore'. These " +
        "entities are reserved for meta data.")
    }

    metrics.asScala.foreach {
      case (key, value) =>
        if (key.startsWith(DoubleUnderscore) && key.endsWith(DoubleUnderscore)) {
          throw new IllegalArgumentException("Unable to persist metrics " +
            s"that start and end with two underscores '$DoubleUnderscore'. These " +
            "entities are reserved for meta data.")
        }
    }
    child.persist(entityId, timestamp, metrics)
  }
}
