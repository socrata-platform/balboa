package com.socrata.metrics.impl

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers
import com.socrata.balboa.metrics.{Metric, Metrics}
import com.socrata.metrics.MetricQueue
import com.socrata.metrics.components.BufferItem
import scala.collection.JavaConverters._

class BufferSpec extends WordSpec with ShouldMatchers with HashMapBufferComponent with TestMessageQueueComponent {
  val testBuffer = Buffer()

  val agg = Metric.RecordType.AGGREGATE
  val abs = Metric.RecordType.ABSOLUTE
  val granularity = MetricQueue.AGGREGATE_GRANULARITY

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

  "when consolidating, the buffer" should {

    "sum metrics if both are of type AGGREGATE" in {
      val consolidatedMetric = testBuffer.consolidate(met2, met1)
      equivalentMetrics(consolidatedMetric, met3) should be (true)
    }
    "replace the first metric with the second if both are of type ABSOLUTE" in {
      val consolidatedMetric = testBuffer.consolidate(met4, met5)
      equivalentMetrics(consolidatedMetric, met5) should be (true)
    }
    "die if the types of the two metrics differ" in {
      val died = try{ testBuffer.consolidate(met1, met5); false } catch { case e:IllegalArgumentException => true}
      died should be (true)
      }
    }

  "when adding, the buffer " should {
    "add the given bufferItem if empty" in {
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
    "add the given bufferItem if not empty and the key doesn't yet exist because of a new entity ID" in {
      val entId = "marc"
      val time = 1L
      testBuffer.add(new BufferItem(entId, met2, time))
      val bufferItem = testBuffer.bufferMap.get(entId + ":" + testBuffer.timeBoundary(time))
      val itemOk = bufferItem match {
        case None => false
        case Some(b) => b.entityId == entId && equivalentMetrics(b.metrics, met2) && b.timestamp == testBuffer.timeBoundary(time)
      }
      itemOk should be (true)
      testBuffer.bufferMap.size should be (2)
    }
    "add the given bufferItem if not empty and the key doesn't yet exist because of a new timeslice" in {
      val entId = "ayn"
      val time = 1L + 2*granularity
      testBuffer.add(new BufferItem(entId, met2, time))
      val bufferItem = testBuffer.bufferMap.get(entId + ":" + testBuffer.timeBoundary(time))
      val itemOk = bufferItem match {
        case None => false
        case Some(b) => b.entityId == entId && equivalentMetrics(b.metrics, met2) && b.timestamp == testBuffer.timeBoundary(time)
      }
      itemOk should be (true)
      testBuffer.bufferMap.size should be (3)
    }
    "update the given bufferItem if not empty and the key does exist and types are the same" in {
      val entId = "marc"
      val time = 1L + granularity/4
      testBuffer.add(new BufferItem(entId, met1, time))
      val bufferItem = testBuffer.bufferMap.get(entId + ":" + testBuffer.timeBoundary(time))
      val itemOk = bufferItem match {
        case None => false
        case Some(b) => b.entityId == entId && equivalentMetrics(b.metrics, met3) && b.timestamp == testBuffer.timeBoundary(time)
      }
      itemOk should be (true)
      testBuffer.bufferMap.size should be (3)
    }
  }


  "after flushing, the buffer" should {
    "be empty when done" in {
      testBuffer.flush()
      testBuffer.bufferMap.size should be (0)
    }
   "have transferred everything to the messageQueue" in {
      testBuffer.messageQueue.dumpingQueue.size should be (3)
   }
  }

  def equivalentMetrics(metrics1:Metrics, metrics2:Metrics) = {
    val keys1 = metrics1.keySet().asScala
    val keys2 = metrics2.keySet().asScala
    keys1.equals(keys2) && keys1.filter(k => !Option(metrics1.get(k)).equals(Option(metrics1.get(k)))).size == 0
  }
}
