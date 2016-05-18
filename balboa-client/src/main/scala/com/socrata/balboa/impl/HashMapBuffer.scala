package com.socrata.balboa.impl

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.impl.JsonMessage
import com.socrata.balboa.metrics.measurements.combining._
import com.socrata.balboa.metrics.{Metric, Metrics}
import com.socrata.metrics.MetricQueue
import com.socrata.metrics.components.{BufferComponent, BufferItem, MessageQueueComponent}
import org.slf4j.LoggerFactory

import collection.mutable
import scala.collection.JavaConverters._

// Not Thread Safe; access must be synchronized by caller (MetricDequeuer)
trait HashMapBufferComponent extends BufferComponent {
  self: MessageQueueComponent =>

  private val log = LoggerFactory.getLogger(this.getClass)

  class Buffer extends BufferLike {
    val bufferMap = mutable.HashMap.empty[String, BufferItem]
    val messageQueue = self.MessageQueue()

    def add(item:BufferItem) {
      val timeslice = timeBoundary(item.timestamp)
      val bufferKey = item.entityId + ":" + timeslice
      val buffered = bufferMap.get(bufferKey)
      val consolidatedBufferItem = buffered match {
        case None => BufferItem(item.entityId, item.metrics, timeslice)
        case Some(bi) => BufferItem(item.entityId, consolidate(bi.metrics, item.metrics), timeslice)
      }
      bufferMap += (bufferKey -> consolidatedBufferItem)
    }

    def consolidate(metrics1:Metrics, metrics2:Metrics): Metrics = {
      val unionKeys = metrics1.keySet().asScala union metrics2.keySet().asScala
      val metricsComb = new Metrics()

      for (key <- unionKeys) {
        val metric1 = Option(metrics1.get(key))
        val metric2 = Option(metrics2.get(key))
        (metric1, metric2) match {
          case (None, None) =>
          case (None, Some(m2)) => metricsComb.put(key, m2)
          case (Some(m1), None) => metricsComb.put(key, m1)
          case (Some(m1), Some(m2)) =>
            if (m1.getType != m2.getType) {
              throw new IllegalArgumentException("Cannot combine differently typed metrics")
            } else {
              m1.getType match {
                case RecordType.ABSOLUTE =>
                  metricsComb.put(key, new Metric(RecordType.ABSOLUTE, (new Absolution).combine(m1.getValue, m2.getValue)))
                case RecordType.AGGREGATE =>
                  metricsComb.put(key, new Metric(RecordType.AGGREGATE, (new Summation).combine(m1.getValue, m2.getValue)))
              }
            }
        }
      }
      metricsComb
    }

    def flush(): Int = {
      val size = bufferMap.size
      for (item <- bufferMap.values){
        val msg = asMessage(item)
        messageQueue.send(msg)
      }
      bufferMap.clear
      size
    }

    private def asMessage(item:BufferItem) = {
      val msg = new JsonMessage()
      msg.setEntityId(item.entityId)
      msg.setMetrics(item.metrics)
      msg.setTimestamp(item.timestamp)
      msg
    }

    def timeBoundary(timestamp:Long): Long = timestamp - (timestamp % MetricQueue.AGGREGATE_GRANULARITY)

    def start() {
      messageQueue.start()
    }

    def stop() {
      if (bufferMap.size > 0) {
        log.info("Flushing " + bufferMap.size + " metrics from BalboaClient buffer before shutting down ...")
        flush()
      }
      messageQueue.stop()
    }

    def size(): Int = bufferMap.size

  }

  def Buffer(): Buffer = new Buffer()
}
