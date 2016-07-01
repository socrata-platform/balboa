package com.socrata.balboa.metrics.data.impl

import java.{util => ju}

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.config.Configuration
import com.socrata.balboa.metrics.data.{DateRange, Period, QueryOptimizer}
import com.socrata.balboa.metrics.{Metric, Metrics, Timeslice}
import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.collection.JavaConverters._

/**
 * DataStore Implementation for Cassandra.
 */
class CassandraDataStore(queryImpl:CassandraQuery = new CassandraQueryImpl(CassandraUtil.initializeContext()))
  extends DataStoreImpl with StrictLogging {
  private val timeSvc = new TimeService()

  @throws(classOf[Exception])
  override def checkHealth(): Unit = {
    queryImpl.checkHealth()
  }

  /**
   * Retrieve an iterator that contains all the entity ids that the pattern
   * string matches. Do not use this outside the admin tool.
   */
  def entities(pattern: String): ju.Iterator[String] = {
    // Yes. This does what you think it does. Admin should only use this. It's nasty stuff.
    // It iterates through all entity ids in both the aggregate and absolute column families
    // of the leastGranular tier, returning them only if they match the filter and have not
    // been seen before (HashSet ent).
    val ent = collection.mutable.HashSet[String]()
    val period = Period.leastGranular(Configuration.get().getSupportedPeriods)
    RecordType.values().iterator.flatMap(
      queryImpl.getAllEntityIds(_,period)
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

  def getValidGranularity(period: Period): Period = {
    val supported = Configuration.get().getSupportedPeriods
    var requestPeriod = Option(period)
    while (requestPeriod.isDefined && !supported.contains(requestPeriod.get)) {
      requestPeriod = Option(requestPeriod.get.moreGranular())
    }
    if (requestPeriod.isEmpty) {
      // We can't find a period which is more granular than the one given, so
      // we do our best.
      requestPeriod = Some(CassandraUtil.mostGranular)
    }
    requestPeriod.get
  }

  /**
   * Return a list of metrics for a period of timeslices over an arbitrary
   * date range, chronologically ascending.
   *
   * e.g. Give me all of the metrics for some entity broken down by hours in
   * the range 2010-01-01 -> 2010-01-31.
   *
   * If an unsupported date is given this will attempt to generate the slice
   * from the next most granular bin which is supported. The resulting TimeSlice
   * will be fixed to the bounds of the requested period and the metrics within
   * each supported bin will be aggregated to the requested bin.
   */
  def slices(entityId: String, period:Period, start: ju.Date, end: ju.Date): ju.Iterator[Timeslice] = {
    val requestPeriod = getValidGranularity(period)
    val dates = new DateRange(start, end).toDates(requestPeriod)
    val timeSlice = CassandraUtil.sliceIterator(queryImpl, entityId, requestPeriod, dates.asScala.toList)
    if (requestPeriod != period) {
      CassandraUtil.rollupSliceIterator(period, timeSlice).asJava
    } else {
      timeSlice.asJava
    }
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
   * If the period is not natively supported one of the smaller granularities will
   * be chosen for the request. This may result in slightly-incorrect data if the
   * smaller granularity does not exactly align on the boundaries of the requested
   * gran. For an unsupported request the number of items returned is equal to the
   * number returned for the supported column (e.g. values are not summarized to
   * match the unsupported bins, should they exist).
   *
   * @see DateRange
   */
  def find(entityId: String, period:Period, date: ju.Date): ju.Iterator[Metrics] = {
    val requestPeriod = getValidGranularity(period)
    val range = DateRange.create(period, date)
    find(entityId, requestPeriod, range.start, range.end)
  }

  /**
   * Find all the summaries of a particular tier between start and end. This
   * is not necessarily the most optimal way to query for an arbitrary range
   * and should only be used when you need to query a specific tier for some
   * reason.
   *
   * If the period is not supported an exception will be thrown.
   */
  def find(entityId: String, period:Period, start: ju.Date, end: ju.Date): ju.Iterator[Metrics] = {
    val query = new DateRange(start, end).toDates(period).asScala.map(date => (date, period))
    CassandraUtil.metricsIterator(queryImpl, entityId, query).asJava
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
    val query = {
      for {
        (period, ranges) <- optimalSlices.toSeq
        range <- ranges.asScala
        date <- range.toDates(period).asScala
      } yield (date, period)
    }.sorted

    // create the query set from the optimal slice
    CassandraUtil.metricsIterator(queryImpl, entityId, query).asJava
  }

  /**
   * Save a set of metrics. The data store is responsible for making sure the
   * persist applies correctly to all supported tiers.
   */
  def persist(entityId: String, timestamp: Long, metrics: Metrics): Unit = {
    // Sort the metrics into aggregates/absolutes
    val absolutes = scala.collection.mutable.HashMap[String, Metric]()
    val aggregates = scala.collection.mutable.HashMap[String, Metric]()
    metrics.asScala.foreach {
      case(key,value) =>
        value.getType match {
          case RecordType.AGGREGATE => aggregates.put(key, value)
          case RecordType.ABSOLUTE => absolutes.put(key, value)
        }
    }
    val start = timeSvc.currentTimeMillis()
    // increment/store metrics in each period
    var period:Option[Period] = Option(CassandraUtil.leastGranular)
    while (period.isDefined && period.get != Period.REALTIME)
    {
      val range:DateRange = DateRange.create(period.get, new ju.Date(timestamp))
      queryImpl.persist(entityId, range.start, period.get, aggregates, absolutes)
      // Skip to the next largest period which we are configured
      // to use.
      period = Option(period.get.moreGranular)
      while (period.isDefined && !CassandraUtil.periods.contains(period.get)) {
        period = period.map(_.moreGranular)
      }
    }
    logger info s"Persisted entity: $entityId  with " + absolutes.size + " absolute and " +
      aggregates.size + " aggregated metrics - took " + (timeSvc.currentTimeMillis() - start) + "ms"
  }

}
