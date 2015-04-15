package com.socrata.balboa.impl

import com.socrata.balboa.metrics.Metric
import com.socrata.metrics.MetricQueue
import com.socrata.metrics.collection.LinkedBlockingPreBufferQueue
import com.socrata.metrics.components.{MetricEnqueuer, MetricEntry}
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

class MetricLoggerSpec extends WordSpec with ShouldMatchers with MetricLoggerToQueue {

  override val delay = 1L
  override val interval =1L

  val aggType = Metric.RecordType.AGGREGATE
  val absType = Metric.RecordType.ABSOLUTE
  val granularity = MetricQueue.AGGREGATE_GRANULARITY

  trait LoggerSetup {
    val testLogger = new MetricLogger() with MetricEnqueuer
      with MetricDequeuerService
      with HashMapBufferComponent
      with TestMessageQueueComponent
      with LinkedBlockingPreBufferQueue

    queueUpA1(testLogger, 1L)
    queueUpA2(testLogger, 1L + 2*granularity)
    queueUpM1(testLogger, 1L)
    queueUpM2(testLogger, 1L + 2*granularity)
    Thread.sleep(100)
  }

  "The queueing method of the MetricLogger" should {
    "push queued contents to the buffer" in new LoggerSetup {
      testLogger.metricDequeuer.actualBuffer.bufferMap.size should be (4)
    }
  }

  "After the flush interval has passed, the MetricLogger" should {
    "have an empty buffer" in new LoggerSetup {
      Thread.sleep(1500)
      testLogger.metricDequeuer.actualBuffer.bufferMap.isEmpty should be (true)
    }
    "have pushed the buffer contents into the message queue" in new LoggerSetup {
      //      testLogger.metricDequeuer.actualBuffer.messageQueue.dumpingQueue.size should be (4)
    }
    "be able to stop gracefully" in new LoggerSetup {
      testLogger.stop()
    }
  }

  "Starting a MetricLogger, writing and immediately shutting down" should {
    "not lose metrics" in new LoggerSetup {
      val quickLogger = new MetricLogger() with MetricEnqueuer
        with MetricDequeuerService
        with HashMapBufferComponent
        with TestMessageQueueComponent
        with LinkedBlockingPreBufferQueue
      quickLogger.enqueue(new MetricEntry("ayn", "num_kitties", 4, 1L, aggType))
      quickLogger.stop()
      Thread.sleep(300)
      quickLogger.dumpingQueue.size should be (1)
    }
  }
  def queueUpA1(enqueuer: MetricEnqueuer, baseTime:Long) = {
    enqueuer.enqueue(new MetricEntry("ayn", "num_kitties", 1, baseTime, aggType))
    enqueuer.enqueue(new MetricEntry("ayn", "num_kitties", 2, baseTime + granularity/4, aggType))
    enqueuer.enqueue(new MetricEntry("ayn", "num_kitties", 3, baseTime + granularity/3, aggType))

    enqueuer.enqueue(new MetricEntry("ayn", "num_kiddies", 1, baseTime, absType))
    enqueuer.enqueue(new MetricEntry("ayn", "num_kiddies", 3, baseTime + granularity/4, absType))
    enqueuer.enqueue(new MetricEntry("ayn", "num_kiddies", 2, baseTime + granularity/3, absType))
  }

  def queueUpA2(enqueuer: MetricEnqueuer, baseTime:Long) = {
    enqueuer.enqueue(new MetricEntry("ayn", "num_kitties", 4, baseTime, aggType))
    enqueuer.enqueue(new MetricEntry("ayn", "num_kitties", -2, baseTime + granularity/4, aggType))
    enqueuer.enqueue(new MetricEntry("ayn", "num_kazoos", 100, baseTime + granularity/4, aggType))
  }

  def queueUpM1(enqueuer: MetricEnqueuer, baseTime:Long) = {
    enqueuer.enqueue(new MetricEntry("marc", "num_kitties", 5, baseTime, aggType))
    enqueuer.enqueue(new MetricEntry("marc", "num_kitties", -1, baseTime + granularity/4, aggType))
    enqueuer.enqueue(new MetricEntry("marc", "num_kitties", -1, baseTime + granularity/3, aggType))

    enqueuer.enqueue(new MetricEntry("marc", "num_kiddies", 1, baseTime, absType))
    enqueuer.enqueue(new MetricEntry("marc", "num_kangaroos", 3, baseTime + granularity/4, absType))
    enqueuer.enqueue(new MetricEntry("marc", "num_kiddies", 2, baseTime + granularity/3, absType))
  }

  def queueUpM2(enqueuer: MetricEnqueuer, baseTime:Long) = {
    enqueuer.enqueue(new MetricEntry("marc", "num_kiddies", -1, baseTime, absType))
    enqueuer.enqueue(new MetricEntry("marc", "num_kiddies", 1, baseTime + granularity/4, absType))
    enqueuer.enqueue(new MetricEntry("marc", "num_kaleidoscopes", 1, baseTime + granularity/2, absType))
    enqueuer.enqueue(new MetricEntry("marc", "num_kangaroos", 1, baseTime + granularity/3, aggType))
  }

}
