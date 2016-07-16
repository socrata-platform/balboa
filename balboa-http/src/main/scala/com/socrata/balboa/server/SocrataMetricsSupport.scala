package com.socrata.balboa.server

import nl.grons.metrics.scala.MetricName
import org.scalatra.metrics.MetricsSupport

/**
  * Wrapper around MetricsSupport that attempts to enforce certain conventions for collecting metrics within
  * Socrata.
  *
  * <br>
  *   Currently enforced conventions:
  *    * Socrata currently uses collectd to collect application metrics exposed via JMX.  Metric names cannot be longer
  *    the 63 characters.  When a metric name exceeds this length, the metric name is truncated at the end until it
  *    meets the character limit requirement. This trait attempts to minimize the full metric name but does
  *    not enforce that a metric name meet this requirement.  This trait does augment the base name of a metric to be
  *    the <a href="https://docs.oracle.com/javase/8/docs/api/java/lang/Class.html">SimpleName</a> of the
  *    SocrataMetricsSupport implementing class.
  *
  */
trait SocrataMetricsSupport extends MetricsSupport {

  /**
    * Sets the base name of the metric to nothing instead of using the class
    * name. Using the class name for metric names makes the metrics fragile in
    * relation to code refactoring. Any class or package renames would change
    * the name of the metric, requiring that dashboards and alarms be updated
    * to compensate. Making the metric names explicit gives the developer
    * greater control, allowing metrics to be renamed, or not, depending on the
    * actions being tracked.
    * <br>
    *   Example:
    *   Rather then looking in Graphite for:
    *     assetinventory->us-west-2->com.socrata.assetinventory.somepackage.TheClassName
    *   You can look for:
    *     assetinventory->us-west-2->metric_name
    */
  override final lazy val metricBaseName: MetricName = MetricName("")

}
