package com.blist.metrics.impl.queue

import com.blist.metrics.impl.queue.MetricsBufferSpecSetup.EmptyMetrics
import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.{Metric, Metrics}
import com.socrata.metrics.MetricQueue
import org.scalatest.{BeforeAndAfterEach, Matchers, WordSpec}

import scala.collection.JavaConverters._

object MetricsBufferSpecSetup {

  trait EmptyMetrics {
    val metrics = new Metrics()
    val metricsBuffer = new MetricsBuffer()
  }

  trait OneElementMetrics extends EmptyMetrics {
    metrics.put("some_metric", new Metric(RecordType.ABSOLUTE, 1))
  }

}

/**
  * Tests for [[MetricsBuffer]].
  *
  * Created by michaelhotan on 2/1/16.
  */
class MetricsBufferSpec extends WordSpec with Matchers with BeforeAndAfterEach {

  "A MetricsBuffer" should {

    "not contain any metrics when none are added" in new EmptyMetrics {
      assert(metricsBuffer.popAll().size() == 0, "Initial Metrics Buffer strangely contains metrics data.")
    }
    "not expose underlying representation" in new EmptyMetrics {
      val c1 = metricsBuffer.popAll()
      c1.add(new MetricsBucket("some_entity_id", metrics, System.currentTimeMillis()))
      assert(metricsBuffer.size() == 0, "Metrics Buffer is exposing internal representation.")
    }
  }

  val agg = Metric.RecordType.AGGREGATE
  val abs = Metric.RecordType.ABSOLUTE
  val granularity = MetricQueue.AGGREGATE_GRANULARITY

  trait BufferSetup {
    val testBuffer = new MetricsBuffer()

    val met1 = new Metrics()
    val met2 = new Metrics()
    val met3 = new Metrics()
    val met4 = new Metrics()
    val met5 = new Metrics()
    val met6 = new Metrics()
    val met7 = new Metrics()
    val met8 = new Metrics()
    val met9 = new Metrics()
    val met10 = new Metrics()

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

    met6.put("num_koalas", new Metric(agg, 4))

    met7.put("num_kiddies", new Metric(agg, 1))

    met8.put("num_kitties", new Metric(agg, 6))

    met9.put("num_kaleidoscopes", new Metric(abs, 0))

    met10.put("num_koalas", new Metric(agg, 4))
    met10.put("num_kiddies", new Metric(agg, 1))
    met10.put("num_kitties", new Metric(agg, 6))
    met10.put("num_kaleidoscopes", new Metric(abs, 0))

    val toInsert : Seq[(String, Metrics, Long)]
    val expected : Seq[MetricsBucket]

    def insert() = {
      toInsert.foreach( (tuple) => {
        (testBuffer.add _).tupled(tuple)
      })
    }

    lazy val buckets: Seq[MetricsBucket] = testBuffer.popAll().asScala.toSeq

    def firstBucket() = {
      buckets.headOption
    }

    def verify() = {
      buckets.size should be (expected.size)

      buckets.zip(expected).filterNot((actualAndExpected: (MetricsBucket, MetricsBucket)) => {
        val (actual, expected) = actualAndExpected
        actual.getId == expected.getId && equivalentMetrics(actual.getData, expected.getData) &&
          actual.getTimeBucket == expected.getTimeBucket
      }).size should be (0)
    }

    def insertAndVerify() = {
      insert
      verify
    }
  }

  trait FullBufferSetup extends BufferSetup {
    val entId1 = "ayn"
    val entId2 = "marc"
    val time = 1L
    val time2 = 1L + 2*granularity

    val original =  Seq(
      (entId1, met1, time),
      (entId1, met1, time),
      (entId2, met2, time),
      (entId1, met2, time2)
    )

    val toInsert = original
  }

  "when adding, the buffer " should {
    "add the given bufferItem if empty" in new BufferSetup {
      val entId = "ayn"
      val time = 1L

      val toInsert = Seq(
        (entId, met1, time)
      )

      val expected = Seq(
        new MetricsBucket(entId, met1, testBuffer.nearestSlice(time))
      )

      insertAndVerify
    }
    "add the given bufferItem if not empty and the key doesn't yet exist because of a new entity ID" in new BufferSetup {
      val entId1 = "ayn"
      val entId2 = "marc"
      val time = 1L

      val toInsert = Seq(
        (entId1, met1, time),
        (entId2, met2, time)
      )

      val expected = Seq(
        new MetricsBucket(entId1, met1, testBuffer.nearestSlice(time)),
        new MetricsBucket(entId2, met2, testBuffer.nearestSlice(time))
      )
    }
    "add the given bufferItem if not empty and the key doesn't yet exist because of a new timeslice" in new FullBufferSetup {
      val metricsForEntId1 = new Metrics
      metricsForEntId1.put("num_kitties", new Metric(agg, -2))
      metricsForEntId1.put("num_kiddies", new Metric(agg, 2))
      metricsForEntId1.put("num_kaleidoscopes", new Metric(abs, 4))

      val expected = Seq(
        new MetricsBucket(entId1, metricsForEntId1, testBuffer.nearestSlice(time)),
        new MetricsBucket(entId2, met2, testBuffer.nearestSlice(time)),
        new MetricsBucket(entId1, met2, testBuffer.nearestSlice(time2))
      )

      insertAndVerify
    }

    "update the given bufferItem if not empty and the key does exist and types are the same" in new FullBufferSetup {
      val entId = "marc"
      val time3 = 1L + granularity/4

      override val toInsert = original ++ Seq(
        (entId, met1, time3)
      )

      val expected = Seq(
        new MetricsBucket(entId, met3, testBuffer.nearestSlice(time3))
      )
    }

    "update buffer with multiple different metrics in different bufferItems" in new FullBufferSetup {
      val time3 = 1L + granularity/4

      override val toInsert = Seq(
        (entId1, met6, time),
        (entId1, met7, time),
        (entId1, met8, time3),
        (entId1, met9, time3)
      )

      val expected = Seq(
        new MetricsBucket(entId1, met10, testBuffer.nearestSlice(time3))
      )

      insertAndVerify
    }
  }

  def equivalentMetrics(metrics1:Metrics, metrics2:Metrics) : Boolean = {
    val keys1 = metrics1.keySet().asScala
    val keys2 = metrics2.keySet().asScala
    keys1.equals(keys2) && keys1.filter(k => {
      !Option(metrics1.get(k)).equals(Option(metrics1.get(k)))
    }).size == 0
  }
}
