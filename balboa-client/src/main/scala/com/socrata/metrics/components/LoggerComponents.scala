package com.socrata.metrics.components

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.metrics.collection.PreBufferQueue

case class MetricEntry(entityId:String, name:String, value:Number, timestamp:Long, recordType:RecordType)

trait MetricLoggerComponent {
  type MetricLogger <: MetricLoggerLike

  /**
   * Interface that defines how to behave like a metric Logger.
   */
  trait MetricLoggerLike {

    /**
     * "Eventually" logs a message into the metrics data pipelines.  Eventually implies that messages might be buffered
     * until a certain capacity or time and then sent over the network.  This will be left up whatever implementation.
     *
     * @param entityId Entity ID
     * @param name The name of the metric
     * @param value The aggregate or absolute value of this metric.
     * @param timestamp The time the metric occurred
     * @param recordType The type of record. IE. Aggregate or Absolute.
     */
    def logMetric(entityId:String, name:String, value:Number, timestamp:Long, recordType:RecordType):Unit

    /**
     * Attempt to stop any outgoing messages.
     */
    def stop():Unit
  }

  /**
   * Method left for legacy reasons.  This function implies that all metric loggers should follow the same structure
   *  messaging structure as many point to messaging buses.  The
   *
   * @param serverName The Messaging Server name
   * @param queueName The name of the queue to place the message.
   * @param backupFileName Where to place the back up files
   * @return MetricLogger
   */
  def MetricLogger(serverName:String, queueName:String, backupFileName:String):MetricLogger
}

trait MetricEnqueuer {
  self: PreBufferQueue =>
  def enqueue(metric:MetricEntry) = self.queue.add(metric)
}
