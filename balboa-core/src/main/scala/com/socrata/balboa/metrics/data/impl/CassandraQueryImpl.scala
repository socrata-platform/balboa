package com.socrata.balboa.metrics.data.impl

import java.io.IOException
import java.{util => ju}

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{BatchStatement, ConsistencyLevel, Row}
import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.data.impl.CassandraUtil.DatastaxContext
import com.socrata.balboa.metrics.data.{BalboaFastFailCheck, Period}
import com.socrata.balboa.metrics.{Metric, Metrics}
import com.typesafe.scalalogging.StrictLogging

import scala.{collection => sc}
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
 * Query Implementation
 */
class CassandraQueryImpl(context: DatastaxContext) extends CassandraQuery with StrictLogging {

  val Key = "key"
  val ColumnOne = "column1"
  val Value = "value"

  @throws(classOf[Exception])
  def checkHealth(): Unit = {
    context.getSession.execute("SELECT now() FROM system.LOCAL;").all()
  }

  def fetch(entityId: String, period: Period, bucket:ju.Date): Metrics = {
    val entityKey: String = CassandraUtil.createEntityKey(entityId, bucket.getTime)
    val ret: Metrics = new Metrics()

    for {
      recordType <- List(RecordType.ABSOLUTE, RecordType.AGGREGATE)
    } yield {
      fetchColumnFamily(recordType, entityKey, period).foreach(row => {
        ret.put(row.getString(ColumnOne), new Metric(recordType, row.getLong(Value)))})
    }

    ret
  }

  def removeTimestamp(key: String): String = key.replaceFirst("-[0-9]+$", "")

  val MaxEntityIds = 100
  /**
   * Returns all the row keys in a tier as an iterator with many, many duplicate strings. This is very slow. Do
   * not use this outside the admin tool.
   */
  def getAllEntityIds(recordType: RecordType, period: Period): Iterator[String] = {
    fastfail.proceedOrThrow()
    try {

      val qb = QueryBuilder.select(Key).distinct()
        .from(context.keyspace, CassandraUtil.getColumnFamily(period, recordType))
        .limit(MaxEntityIds)
        .setConsistencyLevel(ConsistencyLevel.ONE)

      val rows = context.execute(qb)
      val retVal = rows.asScala.map(_.getString(Key)).map(removeTimestamp).iterator

      fastfail.markSuccess()
      retVal
    } catch {
      case e: Exception =>
        val wrapped = new IOException("Error reading entityIds Query:" + recordType + ":" + period + "Cassandra", e)
        fastfail.markFailure(wrapped)
        throw wrapped
    }
  }

  def fetchColumnFamily(recordType: RecordType, entityKey: String, period: Period): Iterator[Row] = {
    fastfail.proceedOrThrow()
    try {

      val qb = QueryBuilder.select().all()
        .from(context.keyspace, CassandraUtil.getColumnFamily(period, recordType))
        .where(QueryBuilder.eq(Key, entityKey))
        .setConsistencyLevel(ConsistencyLevel.ONE)

      val rows = context.execute(qb)

      val retVal = rows.asScala.iterator

      fastfail.markSuccess()
      retVal
    } catch {
      case e: Exception =>
        val wrapped = new IOException("Error reading row " + entityKey + " from " + recordType + ":" + period, e)
        fastfail.markFailure(wrapped)
        throw wrapped
    }
  }

  // scalastyle:off method.length
  def persist(entityId: String,
              bucket: ju.Date,
              period: Period,
              aggregates: sc.Map[String, Metric],
              absolutes: sc.Map[String, Metric]): Unit = {
    val entityKey = CassandraUtil.createEntityKey(entityId, bucket.getTime)
    logger debug s"Using entity/row key $entityKey at period $period"
    fastfail.proceedOrThrow()
    val entityKeyWhere = QueryBuilder.eq(Key, entityKey)

    for {
      sameTypeRecords <- List(absolutes, aggregates).filter(_.nonEmpty)
    } yield {
      // Must execute counter and non-counter separately, since counter batches
      // can only contain counter statements, and non-counter batches can only contain
      // non-counter statements
      //
      // Beyond that, absolutes must go first so that the query doesn't fail
      // after executing the non-idempotent part of the request (i.e. successfully
      // increments aggregate but then fails to write absolute)

      val recordType = sameTypeRecords.iterator.next()._2.getType

      val batchStatement = new BatchStatement(
        if (recordType == RecordType.AGGREGATE) {
          BatchStatement.Type.COUNTER
        } else {
          BatchStatement.Type.LOGGED
        }
      )

      // Initialize the query to work with either AGGREGATE or ABSOLUTE type values
      val table = CassandraUtil.getColumnFamily(period, recordType)

      for {
        (k, v) <- sameTypeRecords
      } yield {
        if (k != "") {
          v.getType match {
            case RecordType.ABSOLUTE =>
              batchStatement.add(
                QueryBuilder.insertInto(table)
                  .value(Key, entityKey).value(ColumnOne, k).value(Value, v.getValue))
            case RecordType.AGGREGATE =>
              batchStatement.add(
                QueryBuilder.update(table)
                  .`with`(QueryBuilder.incr(Value, v.getValue.longValue))
                  .where(entityKeyWhere).and(QueryBuilder.eq(ColumnOne, k)))
          }
        } else {
          logger warn "dropping metric with empty string as column"
        }
      }

      try {
        context.executeUpdate(batchStatement)
        fastfail.markSuccess()
      } catch {
        case e: Exception =>
          val wrapped = new IOException("Error writing metrics " + entityKey + " from " + period, e)
          fastfail.markFailure(wrapped)
          logger.error(s"Error writing metrics $entityKey from $period", e)
          throw wrapped
      }
    }

    fastfail.markSuccess()
  }

  val fastfail: BalboaFastFailCheck = BalboaFastFailCheck.getInstance
}
