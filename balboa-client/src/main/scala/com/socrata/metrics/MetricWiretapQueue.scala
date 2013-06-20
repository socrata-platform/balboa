package com.socrata.metrics

import com.socrata.balboa.metrics.Metric.RecordType

/**
 * Used to log and record the set of de-normalized entities/metrics
 * a set of calls to log* in AbstractMetricQueue would make. The theory
 * behind this is that it's easier to maintain AbstractMetricQueue as the
 *
 */
class MetricWiretapQueue extends AbstractMetricQueue {
  var _record = List[MetricOp]()

  @Override
  def create(entity:IdParts, name:IdParts, value:Long,timestamp:Long, t: RecordType) {
    synchronized {
      _record = new MetricOp(entity, name, t) :: _record
    }
  }

  def record() = _record
}

case class MetricOp(entity:IdParts, name:IdParts, t: RecordType)
