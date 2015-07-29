package com.socrata.balboa.metrics.data.impl

import java.util.Date

import com.socrata.balboa.metrics.Metrics
import com.socrata.balboa.metrics.data.Period

import scala.collection.JavaConverters._
import scala.collection.immutable.TreeMap
import scala.collection.{SortedMap, mutable}

class MemoryDataStore extends DataStoreImpl {
  var map = new TreeMap[(String, Date), Metrics]()

  def entities(pattern: String) = ???

  def entities() = map.keySet.map(_._1).iterator.asJava

  // used by MetricsRest.series
  def slices(entityId: String, period: Period, start: Date, end: Date) = ???

  def find(entityId: String, period: Period, date: Date) = ???

  // used by MetricsRest.get
  def find(entityId: String, period: Period, start: Date, end: Date) =
    find(entityId, start, end)

  // used by MetricsRest.range
  def find(entityId: String, start: Date, end: Date) =
    map.range((entityId, start), (entityId, end)).valuesIterator.asJava

  def persist(entityId: String, timestamp: Long, metrics: Metrics): Unit =
    map += (((entityId, new Date(timestamp)), metrics))
}