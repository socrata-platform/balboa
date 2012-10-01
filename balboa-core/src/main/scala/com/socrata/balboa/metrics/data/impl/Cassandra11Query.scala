package com.socrata.balboa.metrics.data.impl

import com.socrata.balboa.metrics.data.Period
import com.socrata.balboa.metrics.{Metrics, Metric}
import java.{ util => ju }
import scala.collection.Map
import com.socrata.balboa.metrics.Metric.RecordType

/**
 * Queries the underlying datastore
 */
trait Cassandra11Query {
  def getAllEntityIds(recordType:RecordType, period:Period):Iterator[String]

  def fetch(entityKey:String, period:Period, bucket:ju.Date):Metrics

  def persist(entityId:String, bucket:ju.Date, period:Period, aggregates:Map[String, Metric], absolutes:Map[String, Metric])
}
