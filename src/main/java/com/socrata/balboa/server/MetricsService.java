package com.socrata.balboa.server;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.utils.MetricUtils;

import java.io.IOException;
import java.util.*;

public class MetricsService
{
    public List<Object> series(String entityId, Summary.Type type, String field, DateRange range) throws IOException
    {
        DataStore ds = DataStoreFactory.get();

        // TODO: Some way of making this a lazy eval type of list so we don't
        // suck up memory unless we really have to?
        List<Object> list = new ArrayList<Object>();
        Iterator<Summary> iter = ds.find(entityId, type, range.start, range.end);

        while (iter.hasNext())
        {
            Summary summary = iter.next();
            Map<String, Object> data = new HashMap<String, Object>(3);

            DateRange slice = DateRange.create(type, new Date(summary.getTimestamp()));
            data.put("start", slice.start);
            data.put("end", slice.end);

            Object value = summary.getValues().get(field);

            if (value != null)
            {
                data.put(field, value);
                list.add(data);
            }
        }

        return list;
    }

    public List<Map<String, Object>> series(String entityId, Summary.Type type, DateRange range) throws IOException
    {
        DataStore ds = DataStoreFactory.get();

        // TODO: Some way of making this a lazy eval type of list so we don't
        // suck up memory unless we really have to?
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        Iterator<Summary> iter = ds.find(entityId, type, range.start, range.end);

        while (iter.hasNext())
        {
            Summary summary = iter.next();
            Map<String, Object> data = new HashMap<String, Object>(3);

            DateRange slice = DateRange.create(type, new Date(summary.getTimestamp()));
            data.put("start", slice.start);
            data.put("end", slice.end);

            data.put("metrics", summary.getValues());
            list.add(data);
        }

        return list;
    }
    
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
