package com.socrata.balboa.metrics.data.impl

import com.socrata.balboa.metrics.data.{QueryOptimizer, Period, DateRange}
import java.{util => ju}
import com.socrata.balboa.metrics.{Metric, Metrics, Timeslice}
import com.socrata.balboa.metrics.Metric.RecordType
import scala.collection.JavaConverters._
import com.socrata.balboa.metrics.config.Configuration

/**
 * DataStore Implementation for Cassandra 1.1
 *
 *
 *
 */
class Cassandra11DataStore(queryImpl:Cassandra11Query = new Cassandra11QueryImpl(Cassandra11Util.initializeContext()))
  extends DataStoreImpl {

  /**
   * Retrieve an iterator that contains all the entity ids that the pattern
   * string matches.
   */
  def entities(pattern: String): ju.Iterator[String] = {
    // Yes. This does what you think it does. Admin should only use this. It's nasty stuff.
    // It iterates through all entity ids in both the aggregate and absolute column families
    // of the leastGranular tier, returning them only if they match the filter and have not
    // been seen before (HashSet ent).
    val ent = collection.mutable.HashSet[String]()
    val period = Period.leastGranular(Configuration.get().getSupportedPeriods())
    RecordType.values().iterator.flatMap(
      queryImpl.get_allEntityIds(_,period)
          .filter(_.contains(pattern))
          .filter(x => !ent.contains(x) && ent.add(x))
    ).asJava
  }

  /**
   * Retrieve an iterator that encompasses all entity ids for which there are
   * metrics being tracked. Generally speaking, this query is likely to be
   * very expensive and you probably don't want to use it unless you know
   * what you're doing.
   */
  def entities: ju.Iterator[String] = {
    entities("")
  }

  /**
   * Return a list of metrics for a period of timeslices over an arbitrary
   * date range, chronologically ascending.
   *
   * e.g. Give me all of the metrics for some entity broken down by hours in
   * the range 2010-01-01 -> 2010-01-31.
   */
  def slices(entityId: String, period:Period, start: ju.Date, end: ju.Date): ju.Iterator[Timeslice] = {
    val dates = new DateRange(start, end).toDates(period)
    return Cassandra11Util.sliceIterator(queryImpl, entityId, period, dates.asScala.toList).asJava

  }

  /**
   * Given a date and given a summary range period, create the appropriate range
   * for the date (explained below) and perform a query that returns all the
   * summaries for that time period.
   *
   * The range is created by taking the period and finding the boundaries for
   * that period that the date belongs to. For example is the period is "month"
   * and the date is 2010-01-04, the range that will be queried is
   * 2010-01-01 -> 2010-01-31. For more details
   *
   * @see DateRange
   */
  def find(entityId: String, period:Period, date: ju.Date): ju.Iterator[Metrics] = {
    Cassandra11Util.metricsIterator(queryImpl, entityId, Map(period -> List(DateRange.create(period, date).start))).asJava
  }

  /**
   * Find all the summaries of a particular tier between start and end. This
   * is not necessarily the most optimal way to query for an arbitrary range
   * and should only be used when you need to query a specific tier for some
   * reason.
   */
  def find(entityId: String, period:Period, start: ju.Date, end: ju.Date): ju.Iterator[Metrics] = {
    val dates = new DateRange(start, end).toDates(period).asScala
    Cassandra11Util.metricsIterator(queryImpl, entityId, Map(period -> dates.toList)).asJava
  }

  /**
   * Find the total summaries between two particular dates. The query
   * optimizer should plan the query so that start and end align along a date
   * date boundary of your most granular type.
   *
   * @see com.socrata.balboa.metrics.data.Period
   */
  def find(entityId: String, start: ju.Date, end: ju.Date): ju.Iterator[Metrics] = {
    val range:DateRange = new DateRange(start, end)
    val optimalSlices = new QueryOptimizer().optimalSlices(range.start, range.end).asScala
    val query = optimalSlices.flatMap{ case (k,v) => Map(k -> v.asScala.map(_.toDates(k)).flatMap(i => i.asScala).toList)}.toMap
    // create the query set from the optimal slice
    Cassandra11Util.metricsIterator(queryImpl, entityId, query).asJava
  }

  /**
   * Save a set of metrics. The data store is responsible for making sure the
   * persist applies correctly to all supported tiers.
   */
  def persist(entityId: String, timestamp: Long, metrics: Metrics) {
    // Sort the metrics into aggregates/absolutes
    val absolutes = scala.collection.mutable.HashMap[String, Metric]()
    val aggregates = scala.collection.mutable.HashMap[String, Metric]()
    metrics.asScala.foreach {
      case(key,value) => {
        // bad idea
        if (key.startsWith("__") && key.endsWith("__"))
        {
          throw new IllegalArgumentException("Unable to persist metrics " +
            "that start and end with two underscores '__'. These " +
            "entities are reserved for meta data.");
        }
        value.getType match {
          case RecordType.AGGREGATE => aggregates.put(key, value)
          case RecordType.ABSOLUTE => absolutes.put(key, value)
        }
      }
    }

    // increment/store metrics in each period
    var period:Period = Cassandra11Util.leastGranular
    while (period != null && period != Period.REALTIME)
    {
      val range:DateRange = DateRange.create(period, new ju.Date(timestamp))
      queryImpl.persist(entityId, range.start, period, aggregates, absolutes)
      // Skip to the next largest period which we are configured
      // to use.
      period = period.moreGranular()
      while (period != null && !Cassandra11Util.periods.contains(period)) {
        period = period.moreGranular()
      }

    }
  }



}

