package com.socrata.balboa.metrics.data.impl

import com.socrata.balboa.metrics.Metric.RecordType
import com.netflix.astyanax.{Keyspace, AstyanaxContext, MutationBatch}
import com.socrata.balboa.metrics.data.{BalboaFastFailCheck, Period}
import com.socrata.balboa.metrics.{Metrics, Metric}
import scala.collection.mutable.HashMap
import com.netflix.astyanax.model.{ConsistencyLevel, ColumnList}
import scala.collection.JavaConverters._
import java.util.Date
import com.netflix.astyanax.retry.{ExponentialBackoff}
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException
import java.io.IOException
import com.netflix.astyanax.connectionpool.OperationResult

/**
 * Query Implementation
 *
 */
class Cassandra11QueryImpl(context: AstyanaxContext[Keyspace]) extends Cassandra11Query {

  def fetch(entityId: String, period: Period, bucket: Date): Metrics = {
    val entityKey: String = Cassandra11Util.createEntityKey(entityId, bucket.getTime)
    val ret: Metrics = new Metrics()
    // aggregate column family
    val aggResults: ColumnList[String] = fetch_cf(RecordType.AGGREGATE, entityKey, period).getResult
    aggResults.asScala.map(c => ret.put(c.getName, new Metric(Metric.RecordType.AGGREGATE, c.getLongValue())))

    // absolutes column family
    val absResults: ColumnList[String] = fetch_cf(RecordType.ABSOLUTE, entityKey, period).getResult
    absResults.asScala.map(c => ret.put(c.getName, new Metric(Metric.RecordType.ABSOLUTE, c.getLongValue())))

    ret
  }

  /**
   * Returns all the row keys in a tier as an iterator with many, many duplicate strings.
   */
  def get_allEntityIds(recordType: RecordType, period: Period): Iterator[String] = {
    if (!fastfail.proceed()) {
      throw new IOException("fast fail: query immediately because we have failed recently in the past Query:" + recordType + ":" + period)
    }
    try {
      val retVal: Iterator[String] = context.getEntity.prepareQuery(Cassandra11Util.getColumnFamily(period, recordType))
        .setConsistencyLevel(ConsistencyLevel.CL_ONE)
        .withRetryPolicy(new ExponentialBackoff(250, 5)) // initial, max tries
        .getAllRows()
        .setRowLimit(100) // max 100 rows per query to cassandra
        .execute().getResult.iterator().asScala.map {
        row =>
          row.getKey.replaceFirst("-[0-9]+$", "") // remove the timebucket
      }
      fastfail.markSuccess()
      retVal
    } catch {
      case e: Exception => {
        fastfail.markFailure()
        throw new IOException("Error reading entityIds Query:" + recordType + ":" + period + "Cassandra", e)
      }
    }
  }

  def fetch_cf(recordType: RecordType, entityKey: String, period: Period) = {
    if (!fastfail.proceed()) {
      throw new IOException("fast fail: Failing fetch immediately for Query:" + entityKey + " in " + recordType + ":" + period)
    }
    try {
      val retVal: OperationResult[com.netflix.astyanax.model.ColumnList[String]] = context.getEntity.prepareQuery(Cassandra11Util.getColumnFamily(period, recordType))
        .setConsistencyLevel(ConsistencyLevel.CL_ONE)
        .withRetryPolicy(new ExponentialBackoff(250, 5))
        .getKey(entityKey)
        .execute()
      fastfail.markSuccess()
      retVal
    } catch {
      case e: Exception => {
        fastfail.markFailure()
        throw new IOException("Error reading row " + entityKey + " from " + recordType + ":" + period, e)
      }
    }
  }

  def persist(entityId: String, bucket: Date, period: Period, aggregates: HashMap[String, Metric], absolutes: HashMap[String, Metric]) {
    val entityKey = Cassandra11Util.createEntityKey(entityId, bucket.getTime);
    if (!fastfail.proceed()) {
      throw new IOException("fast fail: Failing persist immediately for Query:" + entityKey + " in " + period)
    }

    val m: MutationBatch = context.getEntity.prepareMutationBatch
      .setConsistencyLevel(ConsistencyLevel.CL_ONE)
      .withRetryPolicy(new ExponentialBackoff(250, 5))
    if (!aggregates.isEmpty) {
      var cols = m.withRow(Cassandra11Util.getColumnFamily(period, RecordType.AGGREGATE), entityKey)
      aggregates.foreach(kv =>
        cols = cols.incrementCounterColumn(kv._1, kv._2.getValue.longValue())
      )
    }

    if (!absolutes.isEmpty) {
      var cols = m.withRow(Cassandra11Util.getColumnFamily(period, RecordType.ABSOLUTE), entityKey)
      absolutes.foreach(kv =>
        cols = cols.putColumn(kv._1, kv._2.getValue.longValue())
      )
    }

    try {
      val retVal = m.execute()
      fastfail.markSuccess()
      retVal
    } catch {
      case e: Exception => {
        fastfail.markFailure()
        throw new IOException("Error writing metrics " + entityKey + " from " + period, e)
      }
    }
  }

  val fastfail: BalboaFastFailCheck = BalboaFastFailCheck.getInstance()
}
