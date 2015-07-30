package com.blist.metrics.impl.queue;

import com.socrata.balboa.common.IdParts;
import com.socrata.balboa.common.Metric;

public class MetricNoopQueue extends AbstractJavaMetricQueue
{
    @Override
    public void create(IdParts entity, IdParts name, long value, long timestamp, Metric.RecordType recordType) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
