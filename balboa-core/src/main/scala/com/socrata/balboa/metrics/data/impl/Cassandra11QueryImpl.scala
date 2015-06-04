package com.socrata.balboa.metrics.data.impl

import java.io.IOException
import java.{util => ju}

import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.{ColumnList, ConsistencyLevel, Row}
import com.netflix.astyanax.retry.ExponentialBackoff
import com.netflix.astyanax.{AstyanaxContext, Keyspace, MutationBatch}
import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.data.{BalboaFastFailCheck, Period}
import com.socrata.balboa.metrics.{Metric, Metrics}
import org.apache.commons.logging.LogFactory

import scala.collection.JavaConverters._
import scala.{collection => sc}

/**
 * Query Layer for existing Cassandra Database.
 */
class Cassandra11QueryImpl(context: AstyanaxContext[Keyspace]) extends Cassandra11Query {

  /*
  - TODO Cannot query for a specific metric.
   */

  private val log = LogFactory.getLog(classOf[Cassandra11QueryImpl])

  /**
   * Fetches the metrics for a specific Entity ID, [[Period]], and [[ju.Date]].
   *
   * @param entityId The Entity to fetch metrics for.
   * @param period The [[Period]] to fetch data within.
   * @param bucket The Time bucket for this entity key
   * @return The metrics for this time bucket at the specified Period granularity.
   */
  def fetch(entityId: String, period: Period, bucket:ju.Date): Metrics = {
    val entityKey: String = Cassandra11Util.createEntityKey(entityId, bucket.getTime)
    val ret: Metrics = new Metrics()
    // aggregate column family
    val aggResults: ColumnList[String] = fetchColumnFamily(RecordType.AGGREGATE, entityKey, period).getResult
    aggResults.asScala.map(c => ret.put(c.getName, new Metric(Metric.RecordType.AGGREGATE, c.getLongValue())))

    // absolutes column family
    val absResults: ColumnList[String] = fetchColumnFamily(RecordType.ABSOLUTE, entityKey, period).getResult
    absResults.asScala.map(c => ret.put(c.getName, new Metric(Metric.RecordType.ABSOLUTE, c.getLongValue())))

    ret
  }

  def removeTimestamp(row:Row[String, String]):String = row.getKey.replaceFirst("-[0-9]+$", "")

  /**
   * Returns all the row keys in a tier as an iterator with many, many duplicate strings. This is very slow. Do
   * not use this outside the admin tool.
   */
  def getAllEntityIds(recordType: RecordType, period: Period): Iterator[String] = {
    fastfail.proceedOrThrow()
    try {
      val retVal: Iterator[String] = context.getEntity.prepareQuery(Cassandra11Util.getColumnFamily(period, recordType))
        .setConsistencyLevel(ConsistencyLevel.CL_ONE)
        .withRetryPolicy(new ExponentialBackoff(250, 5)) // initial, max tries
        .getAllRows
        .setRowLimit(100) // max 100 rows per query to cassandra
        .execute().getResult.iterator.asScala.map(removeTimestamp)
      fastfail.markSuccess()
      retVal
    } catch {
      case e: Exception =>
        val wrapped = new IOException("Error reading entityIds Query:" + recordType + ":" + period + "Cassandra", e)
        fastfail.markFailure(wrapped)
        throw wrapped
    }
  }

  private def fetchColumnFamily(recordType: RecordType, entityKey: String, period: Period) = {
    fastfail.proceedOrThrow()
    try {
      val retVal: OperationResult[com.netflix.astyanax.model.ColumnList[String]] = context.getEntity.prepareQuery(Cassandra11Util.getColumnFamily(period, recordType))
        .setConsistencyLevel(ConsistencyLevel.CL_ONE)
        .withRetryPolicy(new ExponentialBackoff(250, 5))
        .getKey(entityKey)
        .execute()
      fastfail.markSuccess()
      retVal
    } catch {
      case e: Exception =>
        val wrapped = new IOException("Error reading row " + entityKey + " from " + recordType + ":" + period, e)
        fastfail.markFailure(wrapped)
        throw wrapped
    }
  }

  def persist(entityId: String, bucket:ju.Date, period: Period, aggregates: sc.Map[String, Metric], absolutes: sc.Map[String, Metric]) {
    val entityKey = Cassandra11Util.createEntityKey(entityId, bucket.getTime)
    log.info("Using entity/row key " + entityKey + " at period " + period)
    fastfail.proceedOrThrow()


    val m:MutationBatch = context.getEntity.prepareMutationBatch
      .setConsistencyLevel(ConsistencyLevel.CL_ONE)
      .withRetryPolicy(new ExponentialBackoff(250, 5))
    if (!aggregates.isEmpty) {
      var cols = m.withRow(Cassandra11Util.getColumnFamily(period, RecordType.AGGREGATE), entityKey)
      for { (k,v) <- aggregates } {
        if (k != ""){ cols = cols.incrementCounterColumn(k, v.getValue.longValue) }
        else { log.warn("dropping metric with empty string as column") }
      }
    }

    if (!absolutes.isEmpty) {
      var cols = m.withRow(Cassandra11Util.getColumnFamily(period, RecordType.ABSOLUTE), entityKey)
      for { (k,v) <- absolutes } {
        if (k != ""){ cols = cols.putColumn(k, v.getValue.longValue) }
        else { log.warn("dropping metric with empty string as column") }
      }
    }

    try {
      val retVal = m.execute()
      fastfail.markSuccess()
      retVal
    } catch {
      case e: Exception =>
        val wrapped = new IOException("Error writing metrics " + entityKey + " from " + period, e)
        fastfail.markFailure(wrapped)
        log.error("Error writing metrics " + entityKey + " from " + period, e)
        throw wrapped
    }
  }

  val fastfail: BalboaFastFailCheck = BalboaFastFailCheck.getInstance
}
