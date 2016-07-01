package com.socrata.metrics.components

import java.util.concurrent.atomic.AtomicBoolean

import com.socrata.balboa.impl.MetricDequeuerService
import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.metrics.collection.PreBufferQueue
import org.slf4j.LoggerFactory

case class MetricEntry(entityId:String, name:String, value:Number, timestamp:Long, recordType:RecordType)

/**
 * Base Metric Logger Component that defines a requirement for
 * a enqueue and dequeue service.
 */
trait BaseMetricLoggerComponent extends MetricLoggerComponent {

  private val log = LoggerFactory.getLogger(this.getClass)

  val delay = 120L
  val interval = 120L

  /**
   * Internal Metric Logger that is based off of [[MetricLogger]].
   */
  class MetricLogger extends MetricLoggerLike {
    self: MetricEnqueuer with MetricDequeuerService =>
    var acceptEnqueues = new AtomicBoolean(true)
    val metricDequeuer = MetricDequeuer()
    val started = metricDequeuer.start(delay, interval)

    /** See [[MetricLoggerLike.logMetric()]] */
    override def logMetric(entityId: String,
                           name: String,
                           value: Number,
                           timestamp: Long,
                           recordType: RecordType): Unit = {
      if (!acceptEnqueues.get()) {
        throw new IllegalStateException(s"${getClass.getSimpleName} has already been stopped")
      }
      enqueue(MetricEntry(entityId, name, value, timestamp, recordType))
    }

    /** See [[MetricLoggerLike.stop()]] */
    override def stop(): Unit = {
      if (acceptEnqueues.compareAndSet(true, false)) {
        log.info(s"Beginning ${getClass.getSimpleName} shutdown")
        metricDequeuer.stop()
      }
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
     * Stop accepting messages, and flush any that have already been accepted.
     */
    def stop():Unit
  }

  // scalastyle: off method.name

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
  def enqueue(metric:MetricEntry): Boolean = self.queue.add(metric)
}
