package com.socrata.metrics.components

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.metrics.collection.PreBufferQueue

case class MetricEntry(entityId:String, name:String, value:Number, timestamp:Long, recordType:RecordType)

trait MetricLoggerComponent {
  type MetricLogger <: MetricLoggerLike
  trait MetricLoggerLike {
    def logMetric(entityId:String, name:String, value:Number, timestamp:Long, recordType:RecordType):Unit
    def stop():Unit
  }

  def MetricLogger(serverName:String, queueName:String, backupFileName:String):MetricLogger
}

trait MetricEnqueuer {
  self: PreBufferQueue =>
  def enqueue(metric:MetricEntry) = self.queue.add(metric)
}
