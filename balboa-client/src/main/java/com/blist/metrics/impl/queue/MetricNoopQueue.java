package com.blist.metrics.impl.queue;

import com.socrata.balboa.metrics.Metric;
import com.socrata.metrics.IdParts;

public class MetricNoopQueue extends AbstractJavaMetricQueue
{
    @Override
    public void create(IdParts entity, IdParts name, long value, long timestamp, Metric.RecordType recordType) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close() throws Exception {
        // Nothing to do because this class does absolutely nothing.
    }
}
