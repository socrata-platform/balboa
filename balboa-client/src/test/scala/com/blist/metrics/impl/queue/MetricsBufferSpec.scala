package com.blist.metrics.impl.queue

import com.blist.metrics.impl.queue.MetricsBufferSpecSetup.EmptyMetrics
import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.{Metric, Metrics}
import org.scalatest.{BeforeAndAfterEach, WordSpec}

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
class MetricsBufferSpec extends WordSpec with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
  }

  "A MetricsBuffer" should {

    "Not contain any metrics when none are added" in new EmptyMetrics {
      assert(metricsBuffer.popAll().size() == 0, "Initial Metrics Buffer strangely contains metrics data.")
    }
    "be immutable" in new EmptyMetrics {
      // This shows that
      val c1 = metricsBuffer.popAll();
      c1.add(new MetricsBag("some_entity_id", metrics, System.currentTimeMillis()))
      assert(metricsBuffer.size() == 0, "Metrics Buffer is exposing internal representation.")
    }

    // TODO Write much more test
  }

}
