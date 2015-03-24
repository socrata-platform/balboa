package com.socrata.metrics

import com.socrata.balboa.metrics.Metric
import java.util.Date
import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.metrics.MetricQueue.{AccessChannel, Action}

abstract class AbstractMetricQueue extends SocrataMetricQueue {

  def create[T <: Number](entity:IdParts, name: IdParts, value:T) {
    create(entity, name, value.longValue(), new Date().getTime)
  }

  def create(entity: IdParts, name: IdParts, value:Long) {
    create(entity, name, value, new Date().getTime, Metric.RecordType.AGGREGATE)
  }

  def create(entity: IdParts, name:IdParts, value:Long, timestamp: Long) {
    create(entity, name, value, timestamp, Metric.RecordType.AGGREGATE)
  }

}