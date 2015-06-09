package com.socrata.balboa.metrics.data.impl

import java.util.Date

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.data.Period
import com.socrata.balboa.metrics.{Metric, Metrics}

import scala.collection.JavaConversions._

/**
 * The Mock Cassandra Query Implementation that actually correlates Fetch and Persist to data store behaviour.
 */
class MemoryCassandra11QueryImpl extends Cassandra11Query {

  var aggregates: Map[Entry, Map[String, Metric]] = Map.empty
  var absolutes : Map[Entry, Map[String, Metric]] = Map.empty

  override def getAllEntityIds(recordType: RecordType, period: Period): Iterator[String] = ???

  override def fetch(entityKey: String, period: Period, bucket: Date): Metrics = {
    val entry = Entry(entityKey, period, bucket)
    new Metrics(this.aggregates.getOrElse(entry, Map.empty) ++ this.absolutes.getOrElse(entry, Map.empty))
  }

  override def persist(entityId: String,
                       bucket: Date,
                       period: Period,
                       aggregates: collection.Map[String, Metric],
                       absolutes: collection.Map[String, Metric]): Unit = {
//    val entry = Entry(entityId, period, bucket)
//    val existingAggregates: collection.Map[String, Metric] = this.aggregates.getOrElse(entry, Map.empty)
//    this.aggregates += (entry -> (existingAggregates ++ aggregates.map{ case (k,v) =>
//      k -> v.combine(existingAggregates.getOrElse(k, new Metric(RecordType.AGGREGATE, 0)))
//    }))
//    this.absolutes ++= absolutes
  }
}

/**
 * Metric Entry.
 *
 * @param entityKey The entity id key
 * @param period The [[Period]] to store data.
 * @param bucket The bucket.
 */
sealed case class Entry(entityKey: String, period: Period, bucket: Date)
