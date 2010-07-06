package com.socrata.balboa.server;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.utils.MetricUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class MetricsService
{
    public Object get(String entityId, Summary.Type type, String field, DateRange range) throws IOException
    {
        DataStore ds = DataStoreFactory.get();

        Iterator<Summary> best = ds.find(entityId, type, range.start, range.end);
        return MetricUtils.summarize(best).get(field);
    }

    public Map<String, Object> get(String entityId, Summary.Type type, DateRange range) throws IOException
    {
        DataStore ds = DataStoreFactory.get();

        Iterator<Summary> best = ds.find(entityId, type, range.start, range.end);
        return MetricUtils.summarize(best);
    }
}
