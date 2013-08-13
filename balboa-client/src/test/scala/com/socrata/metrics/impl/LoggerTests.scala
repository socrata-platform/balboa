package com.socrata.metrics.impl

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers
import com.socrata.metrics.components.{MetricEntry, MetricEnqueuer}
import com.socrata.metrics.collection.LinkedBlockingPreBufferQueue
import com.socrata.balboa.metrics.Metric
import com.socrata.metrics.MetricQueue

class MetricLoggerSpec extends WordSpec with ShouldMatchers
                                        with MetricLoggerToQueue {

  override val delay = 1L
  override val interval =1L

  val testLogger = new MetricLogger() with MetricEnqueuer
                                      with MetricDequeuerService
                                      with HashMapBufferComponent
                                      with TestMessageQueueComponent
                                      with LinkedBlockingPreBufferQueue

  val aggType = Metric.RecordType.AGGREGATE
  val absType = Metric.RecordType.ABSOLUTE
  val granularity = MetricQueue.AGGREGATE_GRANULARITY

  "The queueing method of the MetricLogger" should {
    "push queued contents to the buffer" in {
      queueUpA1(1L)
      queueUpA2(1L + 2*granularity)
      queueUpM1(1L)
      queueUpM2(1L + 2*granularity)
      Thread.sleep(100)
      testLogger.metricDequeuer.actualBuffer.bufferMap.size should be (4)
    }
  }


  "After the flush interval has passed, the MetricLogger" should {
    "have an empty buffer" in {
      Thread.sleep(1500)
      testLogger.metricDequeuer.actualBuffer.bufferMap.isEmpty should be (true)
    }
    "have pushed the buffer contents into the message queue" in {
      testLogger.metricDequeuer.actualBuffer.messageQueue.dumpingQueue.size should be (4)
    }
    "be able to stop gracefully" in {
      testLogger.stop()
    }
  }

  def queueUpA1(baseTime:Long) = {
    testLogger.enqueue(new MetricEntry("ayn", "num_kitties", 1, baseTime, aggType))
    testLogger.enqueue(new MetricEntry("ayn", "num_kitties", 2, baseTime + granularity/4, aggType))
    testLogger.enqueue(new MetricEntry("ayn", "num_kitties", 3, baseTime + granularity/3, aggType))

    testLogger.enqueue(new MetricEntry("ayn", "num_kiddies", 1, baseTime, absType))
    testLogger.enqueue(new MetricEntry("ayn", "num_kiddies", 3, baseTime + granularity/4, absType))
    testLogger.enqueue(new MetricEntry("ayn", "num_kiddies", 2, baseTime + granularity/3, absType))
  }

  def queueUpA2(baseTime:Long) = {
    testLogger.enqueue(new MetricEntry("ayn", "num_kitties", 4, baseTime, aggType))
    testLogger.enqueue(new MetricEntry("ayn", "num_kitties", -2, baseTime + granularity/4, aggType))
    testLogger.enqueue(new MetricEntry("ayn", "num_kazoos", 100, baseTime + granularity/4, aggType))
  }

  def queueUpM1(baseTime:Long) = {
    testLogger.enqueue(new MetricEntry("marc", "num_kitties", 5, baseTime, aggType))
    testLogger.enqueue(new MetricEntry("marc", "num_kitties", -1, baseTime + granularity/4, aggType))
    testLogger.enqueue(new MetricEntry("marc", "num_kitties", -1, baseTime + granularity/3, aggType))

    testLogger.enqueue(new MetricEntry("marc", "num_kiddies", 1, baseTime, absType))
    testLogger.enqueue(new MetricEntry("marc", "num_kangaroos", 3, baseTime + granularity/4, absType))
    testLogger.enqueue(new MetricEntry("marc", "num_kiddies", 2, baseTime + granularity/3, absType))
  }

  def queueUpM2(baseTime:Long) = {
    testLogger.enqueue(new MetricEntry("marc", "num_kiddies", -1, baseTime, absType))
    testLogger.enqueue(new MetricEntry("marc", "num_kiddies", 1, baseTime + granularity/4, absType))
    testLogger.enqueue(new MetricEntry("marc", "num_kaleidoscopes", 1, baseTime + granularity/2, absType))
    testLogger.enqueue(new MetricEntry("marc", "num_kangaroos", 1, baseTime + granularity/3, aggType))
  }

}
