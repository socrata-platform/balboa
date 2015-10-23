package com.socrata.balboa.impl

import com.socrata.balboa.metrics.{Metric, Metrics}
import com.socrata.metrics.MetricQueue
import com.socrata.metrics.components.BufferItem
import org.scalatest.Matchers
import org.scalatest.{BeforeAndAfterEach, WordSpec}

import scala.collection.JavaConverters._

class BufferSpec extends WordSpec with Matchers with HashMapBufferComponent
with TestMessageQueueComponent with BeforeAndAfterEach {

  override protected def afterEach(): Unit = {
    dumpingQueue.clear()
  }

  val agg = Metric.RecordType.AGGREGATE
  val abs = Metric.RecordType.ABSOLUTE
  val granularity = MetricQueue.AGGREGATE_GRANULARITY

  trait BufferSetup {
    val testBuffer = Buffer()

    val met1 = new Metrics()
    val met2 = new Metrics()
    val met3 = new Metrics()
    val met4 = new Metrics()
    val met5 = new Metrics()

    met1.put("num_kitties", new Metric(agg, -1))
    met1.put("num_kiddies", new Metric(agg, 1))
    met1.put("num_kaleidoscopes", new Metric(abs, 4))

    met2.put("num_kitties", new Metric(agg, 2))
    met2.put("num_kiddies", new Metric(agg, 1))

    met3.put("num_kitties", new Metric(agg, 1))
    met3.put("num_kiddies", new Metric(agg, 2))
    met3.put("num_kaleidoscopes", new Metric(abs, 4))

    met4.put("num_kitties", new Metric(abs, 0))

    met5.put("num_kitties", new Metric(abs, 4))
    met5.put("num_kaleidoscopes", new Metric(abs, 0))
    met5.put("num_kazoos", new Metric(abs, 2))
    met5.put("num_kangaroos", new Metric(abs, 0))
  }

  trait FullBufferSetup extends BufferSetup {
    val entId1 = "ayn"
    val entId2 = "marc"
    val time = 1L
    val time2 = 1L + 2*granularity
    testBuffer.add(new BufferItem(entId1, met1, time))
    testBuffer.add(new BufferItem(entId2, met2, time))
    testBuffer.add(new BufferItem(entId1, met2, time2))
  }

  "when consolidating, the buffer" should {

    "sum metrics if both are of type AGGREGATE" in new BufferSetup {
      val consolidatedMetric = testBuffer.consolidate(met2, met1)
      equivalentMetrics(consolidatedMetric, met3) should be (true)
    }
    "replace the first metric with the second if both are of type ABSOLUTE" in new BufferSetup {
      val consolidatedMetric = testBuffer.consolidate(met4, met5)
      equivalentMetrics(consolidatedMetric, met5) should be (true)
    }
    "die if the types of the two metrics differ" in new BufferSetup {
      val died = try{ testBuffer.consolidate(met1, met5); false } catch { case e:IllegalArgumentException => true}
      died should be (true)
    }
  }

  "when adding, the buffer " should {
    "add the given bufferItem if empty" in new BufferSetup {
      val entId = "ayn"
      val time = 1L
      testBuffer.add(new BufferItem(entId, met1, time))
      val bufferItem = testBuffer.bufferMap.get(entId + ":" + testBuffer.timeBoundary(time))
      val itemOk = bufferItem match {
        case None => false
        case Some(b) => b.entityId == entId && equivalentMetrics(b.metrics, met1) && b.timestamp == testBuffer.timeBoundary(time)
      }
      itemOk should be (true)
      testBuffer.bufferMap.size should be (1)
    }
    "add the given bufferItem if not empty and the key doesn't yet exist because of a new entity ID" in new BufferSetup {
      val entId1 = "ayn"
      val entId2 = "marc"
      val time = 1L
      testBuffer.add(new BufferItem(entId1, met1, time))
      testBuffer.add(new BufferItem(entId2, met2, time))
      val bufferItem = testBuffer.bufferMap.get(entId2 + ":" + testBuffer.timeBoundary(time))
      val itemOk = bufferItem match {
        case None => false
        case Some(b) => b.entityId == entId2 && equivalentMetrics(b.metrics, met2) && b.timestamp == testBuffer.timeBoundary(time)
      }
      itemOk should be (true)
      testBuffer.bufferMap.size should be (2)
    }
    "add the given bufferItem if not empty and the key doesn't yet exist because of a new timeslice" in new FullBufferSetup {
      val bufferItem = testBuffer.bufferMap.get(entId1 + ":" + testBuffer.timeBoundary(time2))
      val itemOk = bufferItem match {
        case None => false
        case Some(b) => b.entityId == entId1 && equivalentMetrics(b.metrics, met2) && b.timestamp == testBuffer.timeBoundary(time2)
      }
      itemOk should be (true)
      testBuffer.bufferMap.size should be (3)
    }
    "update the given bufferItem if not empty and the key does exist and types are the same" in new FullBufferSetup {
      val entId = "marc"
      val time3 = 1L + granularity/4
      testBuffer.add(new BufferItem(entId, met1, time3))
      val bufferItem = testBuffer.bufferMap.get(entId + ":" + testBuffer.timeBoundary(time3))
      val itemOk = bufferItem match {
        case None => false
        case Some(b) => b.entityId == entId && equivalentMetrics(b.metrics, met3) && b.timestamp == testBuffer.timeBoundary(time3)
      }
      itemOk should be (true)
      testBuffer.bufferMap.size should be (3)
    }
  }

  "after flushing, the buffer" should {
    "be empty when done" in new FullBufferSetup {
      testBuffer.flush()
      testBuffer.bufferMap.size should be (0)
    }
    "have transferred everything to the messageQueue" in new FullBufferSetup {
      testBuffer.flush()
      dumpingQueue.size should be (3)
    }
  }

  def equivalentMetrics(metrics1:Metrics, metrics2:Metrics) = {
    val keys1 = metrics1.keySet().asScala
    val keys2 = metrics2.keySet().asScala
    keys1.equals(keys2) && keys1.filter(k => !Option(metrics1.get(k)).equals(Option(metrics1.get(k)))).size == 0
  }
}
