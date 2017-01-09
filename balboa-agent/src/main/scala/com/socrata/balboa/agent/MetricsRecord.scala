package com.socrata.balboa.agent

import com.socrata.balboa.metrics.Metric

/**
  * A Metrics Record is an immutable class that represents a metric
  * that occurred with a specific entity-id, name, value, type, and time.
  *
  * Created by michaelhotan on 2/2/16.
  * Converted from Java to Scala on 1/9/2017
  */
class MetricsRecord(
  val entityId: String, val name: String, val value: Number, val timestamp: Long, val metricType: Metric.RecordType) {

  override def equals(o: Any): Boolean = {
    if (this.equals(o)) return true
    if (o == null || (getClass ne o.getClass)) return false
    val that: MetricsRecord = o.asInstanceOf[MetricsRecord]
    timestampsEqual(that) && entityIdsEqual(that) && namesEqual(that) &&
      valuesEqual(that) && (metricType eq that.metricType)
  }

  override def hashCode: Int = {
    var result: Int = hashCodeOrZero(entityId)
    result = 31 * result + hashCodeOrZero(name)
    result = 31 * result + hashCodeOrZero(value)
    result = 31 * result + hashCodeOrZero(metricType)
    31 * result + (timestamp ^ (timestamp >>> 32)).toInt
  }

  override def toString: String = {
    s"MetricsRecord{entityId='$entityId', name='$name', value=$value, type=$metricType, timestamp=$timestamp}"
  }

  private def hashCodeOrZero[A](obj: A): Int = Option(obj).map(_.hashCode).getOrElse(0)

  private def timestampsEqual(other: MetricsRecord): Boolean = {
    timestamp == other.timestamp
  }

  private def entityIdsEqual(other: MetricsRecord): Boolean = {
    Option(entityId).map(_ == other.entityId).getOrElse(other.entityId == null)
  }

  private def namesEqual(other: MetricsRecord): Boolean = {
    Option(name).map(_ == other.name).getOrElse(other.name == null)
  }

  private def valuesEqual(other: MetricsRecord): Boolean = {
    Option(value).map(_ == other.value).getOrElse(other.value == null)
  }
}
