package com.blist.metrics.impl.queue;

import com.socrata.balboa.metrics.Metric;

public class MetricNoopQueue extends AbstractMetricQueue
{
    @Override
    public void create(String entityId, String name, Number value, long timestamp, Metric.RecordType type)
    {
    }
}
