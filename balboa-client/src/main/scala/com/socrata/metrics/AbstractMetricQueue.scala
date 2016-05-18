package com.socrata.metrics

import java.util.Date

import com.socrata.balboa.metrics.Metric

abstract class AbstractMetricQueue extends SocrataMetricQueue {

  def create[T <: Number](entity:IdParts, name: IdParts, value:T): Unit = {
    create(entity, name, value.longValue(), new Date().getTime)
  }

  def create(entity: IdParts, name: IdParts, value:Long): Unit = {
    create(entity, name, value, new Date().getTime, Metric.RecordType.AGGREGATE)
  }

  def create(entity: IdParts, name:IdParts, value:Long, timestamp: Long): Unit = {
    create(entity, name, value, timestamp, Metric.RecordType.AGGREGATE)
  }

}
