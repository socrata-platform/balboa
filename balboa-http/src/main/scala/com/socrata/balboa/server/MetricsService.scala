package com.socrata.balboa.server

import java.util.Date

import com.socrata.balboa.common.{Timeslice, Metrics}
import com.socrata.balboa.common.logging.BalboaLogging
import com.socrata.balboa.metrics.data.impl.PeriodComparator
import com.socrata.balboa.metrics.data.{DataStore, DateRange, Period}
import com.socrata.balboa.metrics.measurements.combining.Summation

import scala.collection.JavaConversions._
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/**
 * Service layer that provides Metrics related functionality.
 */
object MetricsService extends BalboaLogging {

  /*
  Development Notes:

  Class was created to abstract HTTP and actually service functionality.

  - TODO: Reduce the number of parameters per call.
   */

  /**
   * Finds the summary of all metrics of a specific entity ID given a single time window.  The time window is
   * determined by the date and the period.  The date is offset earlier at a distance determined by the period.  For more
   * information see [[DateRange.create()]].
   *
   * <p>
   * Example:
   * <br>
   *   Given an entity id "A" and date: 2010-05-28 16:14:08 with metrics cat => 1
   * <br>
   *   Requesting a period at a date 2010-05-28 16:14:10 with [[Period]] of anything of lower granularity then
   *   [[Period.SECONDLY]] will resolve in metrics cat => 1.
   * </p>
   *
   * @param entityID The Entity ID.
   * @param date The date that determines the Time Slice bucket placement.
   * @param period The period used to offset the date for the specific Time Slice.
   * @param combineFilter Regex filter to combine a specific set of metrics.
   * @param metricFilter Regex filter for extracting specific subset of metrics.
   * @param ds [[DataStore]] that provides access to underlying data.
   * @return A set of Metrics for the Time Slice determined by date + period.
   */
  def period(entityID: String,
             date: Date,
             period: Period,
             combineFilter: Option[String],
             metricFilter: Option[String])(implicit ds: DataStore): Try[Metrics] = {
    Try(require(entityID != null, "Entity ID cannot be null")) flatMap
      (_ => Try(require(date != null, "Date cannot be null"))) flatMap
      (_ => Try(require(period != null, "Period cannot be null"))) flatMap
      (_ => Try(require(combineFilter != null, "Combine Filter cannot be null"))) flatMap
      (_ => Try(require(metricFilter != null, "Metric Filter cannot be null"))) flatMap
      (_ => {
        logger.debug(s"Invoking Period for Entity ID: $entityID, date: $date, period: $period")
        val range = DateRange.create(period, date)
        Try(ds.find(entityID, period, range.getStart, range.getEnd))
      }) flatMap (iterator => {
      var metrics = Metrics.summarize(iterator)
      // Option.foreach optionally uses the evaluation of that option
      combineFilter.foreach(f => metrics = metrics.combine(f))
      metricFilter.foreach(f => metrics = metrics.filter(f))
      logger.debug(s"Completed finding the Period for Entity ID: $entityID, date: $date, period: $period")
      Success(metrics)
    })
  }

  /**
   * Given a specific entity id, target period, and time range find all the metrics for each period ranging from start time to end time.
   *
   * @param entityID The entity ID to obtain metrics for.
   * @param period The interval to collect metrics at.
   * @param dateRange The Date Range for this Data Store Query.
   * @return An iterable collection of Timeslices ranged by the period with respective metrics for that entity ID.
   */
  def series(entityID: String,
             dateRange: DateRange,
             period: Period)(implicit ds: DataStore): Try[Iterable[Timeslice]] =
    Try(require(entityID != null, "Entity ID cannot be null"))
      .flatMap(_ => Try(require(dateRange != null, "Data Range cannot be null")))
      .flatMap(_ => Try(require(period != null, "Period cannot be null")))
      .flatMap(_ => Try(ds.slices(entityID, period, dateRange.getStart, dateRange.getEnd).toIterable))

  /**
   * Returns the consolidation of metrics for a specific entity ID within a specific DateRange.
   *
   * @param entityID The entity ID to obtain metrics for.
   * @param dateRange The date range to bound the metrics query.
   * @param combineFilter The pattern to combine metrics on.
   * @param metricFilter Regex match for filtering through metric to return.
   * @return The Range Query.
   */
  def range(entityID: String,
            dateRange: DateRange,
            combineFilter: Option[String],
            metricFilter: Option[String])(implicit ds: DataStore): Try[Metrics] = {
    Try(require(entityID != null, "Entity ID cannot be null")) flatMap
      (_ =>  Try(require(dateRange != null, "Date Range cannot be null"))) flatMap
      (_ => Try(require(combineFilter != null, "Combine Filter cannot be null"))) flatMap
      (_ => Try(require(metricFilter != null, "Metric Filter cannot be null"))) flatMap
      (_ => Try(ds.find(entityID, dateRange.getStart, dateRange.getEnd))) flatMap
      (iterator => {
        var metrics = Metrics.summarize()
        combineFilter.foreach(f => metrics = metrics.combine(f))
        metricFilter.foreach(f => metrics = metrics.filter(f))
        Success(metrics)
      })
  }

  /**
   * Given a specific Entity ID and Pattern Match find all the metrics for that specific Entity and aggregate them at a specific
   * Period.  This differs from [[series()]] by forcing a level of aggregation for absolute metrics of a Higher Granularity Period.
   * Note that the sample period must be at a higher granularity then the target.
   *
   * <p>
   * For example: If a set of consumed Metrics for an arbitrary entity id "entity_a" all were prefixed with the the word "foo"
   * Then a plausible [[Regex]] would be "foo:*".r.  This would filter through all the metrics that were prefixed by foo and
   * attempt to aggregate them with the desired target period.
   * </p>
   *
   * <br>
   *   Notes
   *   <ul>
   *     <li>If there is a metric name that matches the metricNamePattern but is not absolute or </li>
   *   </ul>
   *
   * @param entityId The entity id to find Metrics For.
   * @param metricNameFilter The Metric name pattern to match against.
   * @param dateRange The Date Range that bounds the time for this request.
   * @param targetPeriod The target period granularity of the metric.
   * @param samplePeriod The period the sample of absolute metrics is matching against.
   * @return Left(Error message), Right (Iterator)
   */
  def forceAggregateSeries(entityId: String,
                     dateRange: DateRange,
                     targetPeriod: Period,
                     metricNameFilter: String,
                     samplePeriod: Period)(implicit ds: DataStore): Try[Iterable[Timeslice]] = {

    // Identify the whether the sample period is of higher granularity.
    new PeriodComparator().compare(samplePeriod, targetPeriod) match {
      case i: Int if i < 0 => // Error Sample period of lower granularity
        Failure(new IllegalArgumentException(s"Sample period $samplePeriod is of a lower " +
          s"granularity level then target period $targetPeriod"))
      case i: Int if i > 0 => // Sample period is of higher granularity.
        MetricsService.series(entityId, dateRange, samplePeriod)(ds) match {
          case Success(sampleTimeSlices) =>
            // Filter out all the metrics we don't want to aggregate.
            val timeSlicesToAggregate = sampleTimeSlices.map(t => {
              val copy = new Timeslice(t.getStart, t.getEnd, new Metrics())
              copy.setMetrics(t.getMetrics.filter(metricNameFilter))
              copy
            })

            // Map the target period to a list of date ranges for the target period.
            val dates = dateRange.toDates(targetPeriod)
            val dateRanges = dates.zip(dates.tail).map(tuple => new DateRange(tuple._1, tuple._2))

            // TODO groupBy is not an optimal method to use. Can be very expensive.
            // Need to reapproach this problem with a more memory optimal solution.  Working with the data structures provided.

            // Group the time slices into the bucket they should fall into and then aggregate them within there respective bucket.
            val forcedAggregatedSlices: Iterable[Timeslice] = timeSlicesToAggregate.toList.groupBy(
              ts => dateRanges.find(dr => dr.includes(new Date(ts.getStart))))
              .map(mapEntry => mapEntry._2.reduce((ts1, ts2) => {
              ts1.addTimeslice(ts2, new Summation())
              ts1
            }))

            // Combine the
            Success(sampleTimeSlices.zip(forcedAggregatedSlices).map(t => {
              t._1.getMetrics.putAll(t._2.getMetrics)
              t._1
            }))
          case Failure(t) => Failure(t)
        }
      //  If the sample period is the same as the target period then pass in the call.
      case i: Int if i == 0 => MetricsService.series(entityId, dateRange, targetPeriod)(ds)
    }}

}
