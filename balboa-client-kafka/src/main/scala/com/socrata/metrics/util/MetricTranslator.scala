package com.socrata.metrics.util

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.{Metric, Metrics}
import com.socrata.metrics.IdParts

/**
 * Translates raw metrics data into
 */
object MetricTranslator {

  /**
   * @
   *
   * @param entity The entity Id for this
   * @param name Name of the Metric to be logged
   * @param value
   * @param timestamp
   * @param recordType
   * @return (Entity Tuple )
   */
  def translate(entity: IdParts, name: IdParts, value: Long, timestamp: Long, recordType: RecordType):
    Option[(String, Long, Metrics)] = {
    (entity, name, value, timestamp, recordType) match {
      case (e: IdParts, n: IdParts, v: Long, t: Long, r: RecordType) =>
        val metrics = new Metrics()
        val metric = new Metric()
        metric.setType(recordType)
        metric.setValue(value)
        metrics.put(name.toString(), metric)
        Some(e.toString(), timestamp, metrics)
      case _=> None
    }
  }

}
