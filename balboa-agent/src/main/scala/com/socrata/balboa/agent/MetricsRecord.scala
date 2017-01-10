package com.socrata.balboa.agent

import com.socrata.balboa.metrics.Metric

/**
  * A Metrics Record is an immutable class that represents a metric
  * that occurred with a specific entity-id, name, value, type, and time.
  *
  * Created by michaelhotan on 2/2/16.
  * Converted from Java to Scala on 1/9/2017
  */
case class MetricsRecord(entityId: String, name: String, value: Number, timestamp: Long, metricType: Metric.RecordType)
