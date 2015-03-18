package com.socrata.balboa.service.kafka.test.util

import java.util
import java.util.Date

import com.socrata.balboa.metrics.data.{DataStore, Period}
import com.socrata.balboa.metrics.{Metrics, Timeslice}

/**
 * Mock Data Store.
 */
class MockDataStore extends DataStore {

  val metricMap: util.Map[String, Metrics]  = new util.Hashtable[String, Metrics]()

  override def persist(entityId: String, timestamp: Long, metrics: Metrics): Unit = metricMap.put(entityId, metrics)

  override def entities(pattern: String): util.Iterator[String] = {throw new UnsupportedOperationException}

  override def slices(entityId: String, period: Period, start: Date, end: Date): util.Iterator[Timeslice] =
  {throw new UnsupportedOperationException}

  override def entities(): util.Iterator[String] = {throw new UnsupportedOperationException}

  override def find(entityId: String, period: Period, date: Date): util.Iterator[Metrics] =
  {throw new UnsupportedOperationException}

  override def find(entityId: String, period: Period, start: Date, end: Date): util.Iterator[Metrics] =
  {throw new UnsupportedOperationException}

  override def find(entityId: String, start: Date, end: Date): util.Iterator[Metrics] =
  {throw new UnsupportedOperationException}

  override def ensureStarted(): Unit = {}

  override def onStart(): Unit = {}

  override def onStop(): Unit = {}

  override def heartbeat(): Unit = {}


  override def toString = s"MockDataStore()"
}
