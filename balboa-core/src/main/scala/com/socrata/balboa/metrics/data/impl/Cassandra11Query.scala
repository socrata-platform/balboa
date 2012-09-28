package com.socrata.balboa.metrics.data.impl

import com.socrata.balboa.metrics.data.{DateRange, Period}
import com.socrata.balboa.metrics.{Metrics, Metric}
import scala.collection.mutable.HashMap
import java.util.Date
import com.socrata.balboa.metrics.Metric.RecordType

/**
 * Queries the underlying datastore
 */
abstract class Cassandra11Query {
  def get_allEntityIds(recordType:RecordType, period:Period):Iterator[String]

  def fetch(entityKey:String, period:Period, bucket:Date):Metrics

  def persist(entityId:String, bucket:Date, period:Period, aggregates:HashMap[String, Metric], absolutes:HashMap[String, Metric])
}
