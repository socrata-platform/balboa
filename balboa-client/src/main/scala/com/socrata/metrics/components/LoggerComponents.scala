package com.socrata.metrics.components

import com.socrata.balboa.impl.MetricDequeuerService
import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.metrics.collection.PreBufferQueue
import org.apache.commons.logging.LogFactory

case class MetricEntry(entityId:String, name:String, value:Number, timestamp:Long, recordType:RecordType)

/**
 * Base Metric Logger Component that defines a requirement for
 * a enqueue and dequeue service.
 */
trait BaseMetricLoggerComponent extends MetricLoggerComponent {
  private val Log = LogFactory.getLog(classOf[BaseMetricLoggerComponent])

  val delay = 120L
  val interval = 120L

  /**
   * Internal Metric Logger that is based off of [[MetricLogger]].
   */
  class MetricLogger extends MetricLoggerLike {
    self: MetricEnqueuer with MetricDequeuerService =>
    var acceptEnqueues = true
    val metricDequeuer = MetricDequeuer()
    val started = metricDequeuer.start(delay, interval)

    /** See [[MetricLoggerLike.logMetric()]] */
    override def logMetric(entityId: String, name: String, value: Number, timestamp: Long, recordType: RecordType): Unit = {
      if (acceptEnqueues)
        enqueue(MetricEntry(entityId, name, value, timestamp, recordType))
      else
        throw new IllegalStateException(s"${getClass.getSimpleName} has already been stopped")
    }

    /** See [[MetricLoggerLike.stop()]] */
    override def stop(): Unit = {
      acceptEnqueues = false
      Log.info(s"Beginning ${getClass.getSimpleName} shutdown")
      metricDequeuer.stop()
    }
  }

}

/**
 * Abstract definition of Metric Logger Component
 */
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
   * Method that creates a Metric Logger using a String interp
   *
   * @param servers The Messaging Server name
   * @param queueName The name of the queue to place the message.
   * @param backupFileName Where to place the back up files
   * @return MetricLogger
   */
  def MetricLogger(servers:String, queueName:String, backupFileName:String):MetricLogger

  /**
   * Creates a Metric Logger using a client defined set of parameters.
   *
   * @return [[MetricLoggerLike]] instance.
   */
  def MetricLogger(): MetricLogger
}

trait MetricEnqueuer {
  self: PreBufferQueue =>
  def enqueue(metric:MetricEntry) = self.queue.add(metric)
}
