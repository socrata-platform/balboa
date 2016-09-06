package com.socrata.balboa.metrics.data.impl

import java.io.IOException
import java.util.Date

import com.socrata.balboa.metrics.Metrics
import com.socrata.balboa.metrics.data.{DataStore, Period}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * Buffers metrics across all metric sources for some
 * time period. This only keeps a single buffer for the
 * current time slice.
 *
 * Metrics with timestamps older than the current slice:
 *    If a metric comes in with a timestamp older than the
 *    current slice it will be passed to the underlying
 *    datastore immediately.
 *
 * Metrics in the current slice:
 *    If a metric comes in with a timestamp within the current
 *    slice it will be aggregated
 *
 * Metrics in the future:
 *    Metrics in the future will trigger a flush of the
 *    buffer and the current slice will be set to the
 *    future timestamp of that metric.
 */
class BufferedDataStore(underlying: DataStore,
                        timeService: TimeService = new TimeService,
                        val bufferGranularity: Long) extends DataStoreImpl {

  val log = LoggerFactory.getLogger(classOf[BufferedDataStore])
  var buffer = new mutable.HashMap[String, Metrics]
  var currentSlice: Long = -1

  @throws[Exception]
  override def checkHealth(): Unit = underlying.checkHealth()

  override def heartbeat(): Unit = {
    val timestamp = timeService.currentTimeMillis()
    val nearestSlice = timestamp - (timestamp % bufferGranularity)
    if (nearestSlice > currentSlice) {
      try {
        flushExpired(timestamp)
      } catch {
        case (e: IOException) =>
          log.error("Unable to flush buffered metrics at regular heartbeat. This is bad.")
      }
    }
  }

  @throws[IOException]
  def flushExpired(timestamp: Long): Unit = {
    buffer.synchronized {
      val nearestSlice = timestamp - (timestamp % bufferGranularity)
      if (nearestSlice > currentSlice) {
        log.info("Flushing " + buffer.size + " entities to underlying datastore from the last " + bufferGranularity + "ms")
        // flush metrics
        buffer.foreach({ case (entity, metrics) =>
          // If a failure occurs in the underlying datastore the exception
          // chain back up and keep the buffer in memory
          log.info("  flushing " + entity)
          underlying.persist(entity, currentSlice, metrics)
        })
        buffer.clear()
        currentSlice = nearestSlice
      }
    }
  }

  @throws[IOException]
  override def persist(entityId: String, timestamp: Long, metrics: Metrics): Unit = {
    buffer.synchronized {
      if (timestamp < currentSlice) {
        // Metrics older than our current slice do not get aggregated.
        underlying.persist(entityId, timestamp, metrics)
      } else {
        flushExpired(timestamp)
        val existing = buffer.get(entityId)
        existing match {
          case Some(existing) =>
            existing.merge(metrics)
            buffer.put(entityId, existing)
          case None =>
            buffer.put(entityId, metrics)
        }
      }
    }
  }

  override def entities() = underlying.entities()
  override def entities(pattern: String) = underlying.entities(pattern)
  override def slices(entityId: String, period: Period, start: Date, end: Date) =
    underlying.slices(entityId, period, start, end)
  override def find(entityId: String, period: Period, start: Date) =
    underlying.find(entityId, period, start)
  override def find(entityId: String, period: Period, start: Date, end: Date) =
    underlying.find(entityId, period, start, end)
  override def find(entityId: String, start: Date, end: Date) =
    underlying.find(entityId, start, end)

  override def onStop(): Unit = heartbeat()
}
