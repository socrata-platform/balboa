package com.socrata.metrics.migrate

import com.socrata.metrics.{ViewUid, MetricOp}

/**
 *
 */
trait MetricTransform {
    def apply(op:MetricOp):MetricOp
}

sealed case class ViewUidMetricTransform(in:ViewUid, out:ViewUid)
