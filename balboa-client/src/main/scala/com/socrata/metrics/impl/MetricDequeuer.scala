package com.socrata.metrics.impl

import com.socrata.balboa.metrics.{Metric, Metrics}
import com.socrata.metrics.collection.PreBufferQueue
import com.socrata.metrics.components.{MetricEntry, BufferItem, BufferComponent}
import java.util.concurrent.{RejectedExecutionException, TimeUnit, Executors, ExecutorService}
import org.apache.commons.logging.LogFactory

trait MetricDequeuerService {
  self: BufferComponent with PreBufferQueue =>

  val timeout = 500L
  private val log = LogFactory.getLog(classOf[MetricDequeuer])

  class MetricDequeuer {
    var keepRunning = true
    val dequeueExecutor = Executors.newFixedThreadPool(1)
    val flushExecutor = Executors.newScheduledThreadPool(1)
    val actualBuffer = Buffer()

    class PreBufferDequeuer(val buffer:Buffer) extends Runnable {
      def run() {
        while(keepRunning) {
          // if need be, we can add a cushion to prevent context switching, e.g. only take if (queue.size > 10)
          queue.take() match {
            case Some(m) => buffer.synchronized { buffer.add(asBufferItem(m)) }
            case None =>
          }
        }
      }
    }

    class BufferFlusher(val buffer:Buffer) extends Runnable {
      override def run() {
        val numFlushed = buffer.synchronized { buffer.flush() }
        val queueSize = queue.size
        if (numFlushed < queueSize) log.warn("The metric queue contains " + queueSize + " elements; the last buffer flush emptied out " + numFlushed + " elements.")
      }
    }

    private def shutDown(service:ExecutorService) {
      if (!keepRunning) {
        service.shutdown()
        if(service.awaitTermination(timeout, TimeUnit.MILLISECONDS)) service.shutdownNow()
        else if(service.awaitTermination(timeout, TimeUnit.MILLISECONDS)) service.shutdownNow()
        else service.shutdownNow()
      }
    }

    private def asBufferItem(m:MetricEntry) = {
      val metrics = new Metrics()
      val metric = new Metric(m.recordType, m.value)
      metrics.put(m.name, metric)
      BufferItem(m.entityId, metrics, m.timestamp)
    }

    def start(delay:Long, interval:Long) = {
      actualBuffer.start()
      flushExecutor.scheduleWithFixedDelay(new BufferFlusher(actualBuffer), delay, interval, TimeUnit.SECONDS)
      dequeueExecutor.submit(new PreBufferDequeuer(actualBuffer))
    }

    def stop() {
      keepRunning = false
      shutDown(dequeueExecutor)
      shutDown(flushExecutor)
      actualBuffer.stop()
    }
  }

  def MetricDequeuer() = new MetricDequeuer()
}
