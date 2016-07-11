package com.socrata.balboa.impl

import java.util.concurrent.{Future, Executors, TimeUnit}

import com.socrata.balboa.metrics.{Metric, Metrics}
import com.socrata.metrics.collection.PreBufferQueue
import com.socrata.metrics.components.{BufferComponent, BufferItem, MetricEntry}
import org.slf4j.LoggerFactory

trait MetricDequeuerService {
  self: BufferComponent with PreBufferQueue =>

  val timeout = 500L
  private val log = LoggerFactory.getLogger(this.getClass)

  class MetricDequeuer {
    var keepRunning = true
    val dequeueExecutor = Executors.newFixedThreadPool(1)
    val flushExecutor = Executors.newScheduledThreadPool(1)
    val actualBuffer = Buffer()

    class PreBufferDequeuer(val buffer:Buffer) extends Runnable {
      def run(): Unit = {
        while(keepRunning) take()
        while(!queue.isEmpty) take()
      }

      def take(): Unit = {
        // if need be, we can add a cushion to prevent context switching, e.g. only take if (queue.size > 10)
        Option(queue.poll(1, TimeUnit.SECONDS)) match {
          case Some(m) => buffer.synchronized { buffer.add(asBufferItem(m)) }
          case None =>
        }
      }
    }

    class BufferFlusher(val buffer:Buffer) extends Runnable {
      override def run(): Unit = {
        val numFlushed = buffer.synchronized { buffer.flush() }
        val queueSize = queue.size
        if (numFlushed < queueSize) log.warn(
          s"The metric queue contains $queueSize elements; the last buffer flush emptied out $numFlushed elements.")
      }
    }

    private def asBufferItem(m:MetricEntry) = {
      val metrics = new Metrics()
      val metric = new Metric(m.recordType, m.value)
      metrics.put(m.name, metric)
      BufferItem(m.entityId, metrics, m.timestamp)
    }

    def start(delay:Long, interval:Long): Future[_] = {
      actualBuffer.start()
      flushExecutor.scheduleWithFixedDelay(new BufferFlusher(actualBuffer), delay, interval, TimeUnit.SECONDS)
      dequeueExecutor.submit(new PreBufferDequeuer(actualBuffer))
    }

    def stop(): Unit = {
      keepRunning = false
      dequeueExecutor.shutdown()
      while(!dequeueExecutor.awaitTermination(timeout, TimeUnit.MILLISECONDS))
        log.info("Emptying out BalboaClient queue; " + queue.size + " elements left.")
      dequeueExecutor.shutdownNow()
      flushExecutor.shutdown()
      while(!flushExecutor.awaitTermination(timeout, TimeUnit.MILLISECONDS))
        log.info("Allowing BalboaClient buffer to finish flushing; " + actualBuffer.size() + " elements left.")
      flushExecutor.shutdownNow()
      actualBuffer.stop()
    }
  }

  // scalastyle:off method.name
  def MetricDequeuer(): MetricDequeuer = new MetricDequeuer()
}
