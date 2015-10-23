package com.socrata.balboa.agent

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.metrics.{IdParts, MetricQueue}

class ConsoleMetricQueue extends MetricQueue {

  /**
   * Interface for receiving a Metric
   *
   * @param entity Entity which this Metric belongs to (ex: a domain).
   * @param name Name of the Metric to store.
   * @param value Numeric value of this metric.
   * @param timestamp Time when this metric was created.
   * @param recordType Type of metric to add, See [[com.socrata.balboa.metrics.Metric.RecordType]] for more information.
   */
  override def create(entity: IdParts, name: IdParts, value: Long, timestamp: Long, recordType: RecordType): Unit =
    System.out.println(s"Entity: $entity Name: $name Value: $value Timestamp: $timestamp Record Type: $recordType")
}
