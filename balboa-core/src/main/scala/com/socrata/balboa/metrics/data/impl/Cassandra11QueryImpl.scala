package com.socrata.balboa.metrics.data.impl

import java.io.IOException
import java.nio.charset.Charset
import java.{util => ju}

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{BatchStatement, ConsistencyLevel, Row}
import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.data.impl.Cassandra11Util.DatastaxContext
import com.socrata.balboa.metrics.data.{BalboaFastFailCheck, Period}
import com.socrata.balboa.metrics.{Metric, Metrics}
import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.{collection => sc}
import scala.collection.JavaConversions.asScalaIterator

/**
 * Query Implementation
 *
 */
class Cassandra11QueryImpl(context: DatastaxContext) extends Cassandra11Query with StrictLogging {

  def fetch(entityId: String, period: Period, bucket:ju.Date): Metrics = {
    val entityKey: String = Cassandra11Util.createEntityKey(entityId, bucket.getTime)
    val ret: Metrics = new Metrics()

    for (recordType <- List(RecordType.ABSOLUTE, RecordType.AGGREGATE)) {
      fetch_cf(recordType, entityKey, period).foreach(row => {
        ret.put(row.getString("column1"), new Metric(recordType, row.getLong("value")))})
    }

    /*
    // aggregate column family
    val aggResults: ColumnList[String] = fetch_cf(RecordType.AGGREGATE, entityKey, period).getResult
    aggResults.asScala.map(c => ret.put(c.getName, new Metric(Metric.RecordType.AGGREGATE, c.getLongValue())))

    // absolutes column family
    val absResults: ColumnList[String] = fetch_cf(RecordType.ABSOLUTE, entityKey, period).getResult
    absResults.asScala.map(c => ret.put(c.getName, new Metric(Metric.RecordType.ABSOLUTE, c.getLongValue())))
    */

    ret
  }

  def removeTimestamp(key: String): String = key.replaceFirst("-[0-9]+$", "")

  /**
   * Returns all the row keys in a tier as an iterator with many, many duplicate strings. This is very slow. Do
   * not use this outside the admin tool.
   */
  def getAllEntityIds(recordType: RecordType, period: Period): Iterator[String] = {
    fastfail.proceedOrThrow()
    try {

      val qb = QueryBuilder.select("key").distinct()
        .from(context.keyspace, Cassandra11Util.getColumnFamily(period, recordType))
        .limit(100)
        .setConsistencyLevel(ConsistencyLevel.ONE)

      val rows = context.newSession.execute(qb).all()
      val retVal = asScalaIterator(rows.iterator()).map(_.getString("key")).map(removeTimestamp)

      /*
      val retVal: Iterator[String] = context.getEntity.prepareQuery(Cassandra11Util.getColumnFamily(period, recordType))
        .setConsistencyLevel(ConsistencyLevel.CL_ONE)
        .withRetryPolicy(new ExponentialBackoff(250, 5)) // initial, max tries
        .getAllRows
        .setRowLimit(100) // max 100 rows per query to cassandra
        .execute().getResult.iterator.asScala.map(removeTimestamp)
      */

      fastfail.markSuccess()
      retVal
    } catch {
      case e: Exception =>
        val wrapped = new IOException("Error reading entityIds Query:" + recordType + ":" + period + "Cassandra", e)
        fastfail.markFailure(wrapped)
        throw wrapped
    }
  }

  def fetch_cf(recordType: RecordType, entityKey: String, period: Period): Iterator[Row] = {
    fastfail.proceedOrThrow()
    try {

      val qb = QueryBuilder.select().all()
        .from(context.keyspace, Cassandra11Util.getColumnFamily(period, recordType))
        .where(QueryBuilder.eq("key", entityKey))
        .setConsistencyLevel(ConsistencyLevel.ONE)

      val rows = context.newSession.execute(qb).all()

      val retVal = asScalaIterator(rows.iterator())

      /*
      val retVal: OperationResult[com.netflix.astyanax.model.ColumnList[String]] = context.getEntity
        .prepareQuery(Cassandra11Util.getColumnFamily(period, recordType))
        .setConsistencyLevel(ConsistencyLevel.CL_ONE)
        .withRetryPolicy(new ExponentialBackoff(250, 5))
        .getKey(entityKey)
        .execute()
      */

      fastfail.markSuccess()
      retVal
    } catch {
      case e: Exception =>
        val wrapped = new IOException("Error reading row " + entityKey + " from " + recordType + ":" + period, e)
        fastfail.markFailure(wrapped)
        throw wrapped
    }
  }

  def persist(entityId: String,
              bucket: ju.Date,
              period: Period,
              aggregates: sc.Map[String, Metric],
              absolutes: sc.Map[String, Metric]): Unit = {
    val entityKey = Cassandra11Util.createEntityKey(entityId, bucket.getTime)
    logger debug s"Using entity/row key $entityKey at period $period"
    fastfail.proceedOrThrow()

    val batchStatement = new BatchStatement(BatchStatement.Type.LOGGED)

    val entityKeyWhere = QueryBuilder.eq("key", entityKey)

    /*
    val m:MutationBatch = context.getEntity.prepareMutationBatch
      .setConsistencyLevel(ConsistencyLevel.CL_ONE)
      .withRetryPolicy(new ExponentialBackoff(250, 5))
    */

    for (sameTypeRecords <- List(aggregates, absolutes)) {
      if (sameTypeRecords.nonEmpty) {
        // Initialize the query to work with either AGGREGATE or ABSOLUTE type values
        val table =  Cassandra11Util.getColumnFamily(period, sameTypeRecords.iterator.next()._2.getType)

        for {(k, v) <- sameTypeRecords} {
          if (k != "") {
            v.getType match {
              case RecordType.ABSOLUTE =>
                batchStatement.add(
                   QueryBuilder.insertInto(table)
                     .value("key", entityKey).value("column1", k).value("value", v.getValue))
              case RecordType.AGGREGATE =>
                batchStatement.add(
                  QueryBuilder.update(table)
                    .`with`(QueryBuilder.incr("value", v.getValue.longValue))
                    .where(entityKeyWhere).and(QueryBuilder.eq("column1", k)))
            }
          } else {
            logger warn "dropping metric with empty string as column"
          }
        }
      }
    }

    /*
    if (aggregates.nonEmpty) {
      var cols = m.withRow(Cassandra11Util.getColumnFamily(period, RecordType.AGGREGATE), entityKey)
      for { (k,v) <- aggregates } {
        if (k != ""){ cols = cols.incrementCounterColumn(k, v.getValue.longValue) }
        else { logger warn("dropping metric with empty string as column") }
      }
    }
    */

    /*
    if (absolutes.nonEmpty) {
      var cols = m.withRow(Cassandra11Util.getColumnFamily(period, RecordType.ABSOLUTE), entityKey)
      for { (k,v) <- absolutes } {
        if (k != ""){ cols = cols.putColumn(k, v.getValue.longValue) }
        else { logger warn("dropping metric with empty string as column") }
      }
    }
    */

    try {
      val retVal = context.newSession.execute(batchStatement)
      retVal.all()
      fastfail.markSuccess()
      /*
      val retVal = m.execute()
      fastfail.markSuccess()
      retVal.getResult
      */
    } catch {
      case e: Exception =>
        val wrapped = new IOException("Error writing metrics " + entityKey + " from " + period, e)
        fastfail.markFailure(wrapped)
        logger.error(s"Error writing metrics $entityKey from $period", e)
        throw wrapped
    }
  }

  val fastfail: BalboaFastFailCheck = BalboaFastFailCheck.getInstance
}
