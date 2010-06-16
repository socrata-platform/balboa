package com.socrata.balboa.server;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.measurements.Configuration;
import com.socrata.balboa.metrics.measurements.MetricReader;
import com.socrata.balboa.metrics.measurements.combining.Combinator;
import com.socrata.balboa.metrics.measurements.preprocessing.JsonPreprocessor;

import java.io.IOException;
import java.util.Map;

public class MetricsService
{
    public Object get(String entityId, Summary.Type type, String field, Combinator combinator, DateRange range) throws IOException
    {
        DataStore ds = DataStoreFactory.get();
        MetricReader reader = new MetricReader();

        return reader.read(entityId, field, type, range, ds, new JsonPreprocessor(), combinator);
    }

    public Map<String, Object> get(String entityId, Summary.Type type, Configuration config, DateRange range) throws IOException
    {
        DataStore ds = DataStoreFactory.get();
        MetricReader reader = new MetricReader();

        return reader.read(entityId, config, type, range, ds);
    }
}
