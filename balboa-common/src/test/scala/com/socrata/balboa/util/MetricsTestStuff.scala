package com.socrata.balboa.util

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.impl.JsonMessage
import com.socrata.balboa.metrics.{Metric, Metrics}

/**
 * Premade testing objects.
 */
object MetricsTestStuff {

  /**
   * Metrics
   */
  trait TestMetrics {
    val emptyMetrics = new Metrics()
    val oneElemMetrics = metrics(("cats", metric(1)))
    val manyElemMetrics = metrics(("cats", metric(1)),
      ("giraffes", metric(2)),
      ("dogs", metric(3)),
      ("penguins", metric(1000000)),
      ("monkeys", metric(5)),
      ("rhinos", metric(6)))
  }

  /**
   * Precomposed messages.
   */
  trait TestMessages extends TestMetrics {
    val emptyMessage = message("empty", emptyMetrics, 1)
    val oneElemMessage = message("empty", oneElemMetrics, 1)
    val manyElemMessage = message("empty", manyElemMetrics, 1)
  }

  def metric(value: Number, t: RecordType = RecordType.AGGREGATE): Metric = {
    val m = new Metric()
    m.setType(t)
    m.setValue(value)
    m
  }

  def metrics(metrics: (String, Metric)*): Metrics = {
    val ms = new Metrics()
    metrics.foreach(m => ms.put(m._1, m._2))
    ms
  }

  def message(entityId: String, metrics: Metrics, time: Long = 1): JsonMessage = {
    val m = new JsonMessage()
    m.setEntityId(entityId)
    m.setTimestamp(time)
    m.setMetrics(metrics)
    m
  }

}
