package com.blist.metrics.impl.queue;

import com.socrata.balboa.metrics.Metric;
import com.socrata.metrics.AbstractMetricQueue;

import java.util.Date;

/**
 * Scala binding for AbstractMetricQueue.  See {@link com.socrata.metrics.AbstractMetricQueue}
 */
public abstract class AbstractJavaMetricQueue extends AbstractMetricQueue {

    @Override
    public Metric.RecordType create$default$5() {
        return Metric.RecordType.AGGREGATE;
    }

    @Override
    public long create$default$4() {
        return new Date().getTime();
    }
}
