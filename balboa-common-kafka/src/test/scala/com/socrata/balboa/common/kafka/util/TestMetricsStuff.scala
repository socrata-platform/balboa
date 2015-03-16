package com.socrata.balboa.common.kafka.util

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.{Metric, Metrics}
import com.socrata.balboa.metrics.impl.JsonMessage

/**
 * Premade testing objects.
 */
object TestMetricsStuff {

  /**
   * Metrics
   */
  trait TestMetrics {
    val emptyMetrics = new Metrics()
    val oneElemMetrics = new Metrics()
    oneElemMetrics.put("cats", metric(1))
    val manyElemMetrics = new Metrics()
    manyElemMetrics.put("cats", metric(1))
    manyElemMetrics.put("giraffes", metric(2))
    manyElemMetrics.put("dogs", metric(3))
    manyElemMetrics.put("penguins", metric(4))
    manyElemMetrics.put("monkeys", metric(5))
    manyElemMetrics.put("rhinos", metric(6))
  }

  /**
   * Precomposed messages.
   */
  trait TestMessages extends TestMetrics {
    val emptyMessage = new JsonMessage()
    emptyMessage.setEntityId("empty")
    emptyMessage.setTimestamp(1)
    emptyMessage.setMetrics(emptyMetrics)

    val oneElemMessage = new JsonMessage()
    oneElemMessage.setEntityId("oneElem")
    oneElemMessage.setTimestamp(1)
    oneElemMessage.setMetrics(oneElemMetrics)

    val manyElemMessage = new JsonMessage()
    manyElemMessage.setEntityId("many")
    manyElemMessage.setTimestamp(1)
    manyElemMessage.setMetrics(manyElemMetrics)
  }

  def metric(value: Number, t: RecordType = RecordType.AGGREGATE): Metric = {
    val m = new Metric()
    m.setType(t)
    m.setValue(value)
    m
  }

}
