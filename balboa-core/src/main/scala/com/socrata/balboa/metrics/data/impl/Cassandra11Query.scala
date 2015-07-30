package com.socrata.balboa.metrics.data.impl

import java.{util => ju}

import com.socrata.balboa.common.Metric.RecordType
import com.socrata.balboa.common.{Metric, Metrics}
import com.socrata.balboa.metrics.data.Period

import scala.{collection => sc}

/**
 * Queries the underlying datastore.
 */
trait Cassandra11Query {
  def getAllEntityIds(recordType:RecordType, period:Period):Iterator[String]

  def fetch(entityKey:String, period:Period, bucket:ju.Date):Metrics

  def persist(entityId:String, bucket:ju.Date, period:Period, aggregates:sc.Map[String, Metric], absolutes:sc.Map[String, Metric])
}
