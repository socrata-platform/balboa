package com.socrata.metrics.components

/**
 * Created by Michael Hotan on 2/18/15.
 *
 * Specification that defines the interface for logging metrics to our
 * internal service.
 */
trait MetricLogger {


  def log(metric: MetricEntry): Unit

}
