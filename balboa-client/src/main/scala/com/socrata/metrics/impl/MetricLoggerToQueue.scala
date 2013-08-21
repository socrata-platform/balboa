package com.socrata.metrics.impl

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.metrics.components.{MetricEntry, MetricEnqueuer, MetricLoggerComponent}
import com.socrata.metrics.collection.LinkedBlockingPreBufferQueue

trait ServerInformation {
  def activeServer:String
  def activeQueue:String
  def backupFile:String
}

trait MetricLoggerToQueue extends MetricLoggerComponent {
  val delay = 120L
  val interval = 120L

  class MetricLogger() extends MetricLoggerLike {
    self: MetricEnqueuer with MetricDequeuerService =>

    var acceptEnqueues = true
    val metricDequeuer = MetricDequeuer()
    val started = metricDequeuer.start(delay, interval)

    def logMetric(entityId:String, name:String, value:Number, timestamp:Long, recordType:RecordType) {
      if (acceptEnqueues)
        enqueue(MetricEntry(entityId, name, value, timestamp, recordType))
      else
        throw new IllegalStateException("MetricLogger has already been stopped")
    }

    def stop() {
      acceptEnqueues = false
      metricDequeuer.stop()
    }

  }

  def MetricLogger(serverName:String, queueName:String, backupFileName:String):MetricLogger = {
    new MetricLogger() with MetricEnqueuer
                       with MetricDequeuerService
                       with HashMapBufferComponent
                       with ActiveMQueueComponent
                       with LinkedBlockingPreBufferQueue
                       with BufferedStreamEmergencyWriterComponent
                       with ServerInformation { lazy val activeServer = serverName; lazy val activeQueue = queueName; lazy val backupFile = backupFileName}
  }

}





