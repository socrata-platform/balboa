package com.socrata.balboa.server.rest

import java.util.Date

import com.socrata.balboa.metrics.data.{DateRange, Period}

/**
 * Base Parameters for a Metrics Service Request.
 *
 * @param entityID The Entity ID to find the request for.
 */
sealed abstract class Parameters(entityID: String)

/**
 * Parameters for Period Method
 *
 * @param entityID The Entity ID to find the request for.
 * @param period The Period that determines the granularity of the range
 * @param date The Date The date that determines the date range using the argument period.
 * @param combineFilter The pattern to match with metrics to combine.
 * @param metricFilter The pattern to match for metrics to be filtered after combination.
 */
case class PeriodParameters(entityID: String,
                            period: Period,
                            date: Date,
                            combineFilter: Option[String],
                            metricFilter: Option[String]) extends Parameters(entityID)

/**
 * Parameters for series based requests.
 *
 * @param entityID The Entity ID to find the request for.
 * @param dateRange Date Range.
 * @param period The [[Period]] to find the request for.
 */
case class SeriesParameters(entityID: String,
                            dateRange: DateRange,
                            period: Period) extends Parameters(entityID)

/**
 * Parameters for Range based requests.
 *
 * @param entityID The Entity ID to find the request for.
 * @param dateRange The [[DateRange]] for this particular Range.
 * @param combineFilter The Regex Filter to Combine Metrics by name with.
 * @param metricFilter The metric name pattern to return ranged queries for.
 */
case class RangeParameters(entityID: String,
                           dateRange: DateRange,
                           combineFilter: Option[String],
                           metricFilter: Option[String]) extends Parameters(entityID)

/**
 * Parameters for [[com.socrata.balboa.server.MetricsService.forceAggregateSeries()]].
 *
 * @param entityID The Entity ID to find the request for.
 * @param dateRange Date Range.
 * @param period The [[Period]] to find the request for.
 * @param samplePeriod The [[Period]] the period of metrics to poll for.
 * @param metricFilter The filter of metrics to return.
 */
case class ForceAggregateSeriesParameters(entityID: String,
                                          dateRange: DateRange,
                                          period: Period,
                                          samplePeriod: Period,
                                          metricFilter: String) extends Parameters(entityID)