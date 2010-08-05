package com.socrata.balboa.server;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.data.QueryOptimizer;
import com.socrata.balboa.metrics.measurements.combining.Combinator;
import com.socrata.balboa.metrics.measurements.combining.Summation;
import com.socrata.balboa.metrics.utils.MetricUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.*;

public class MetricsService
{
    private static Log log = LogFactory.getLog(MetricsService.class);

    public Object range(String entityId, String[] combine, DateRange range) throws IOException
    {
        QueryOptimizer optimizer = new QueryOptimizer();
        Map<DateRange.Type,  List<DateRange>> slices = optimizer.optimalSlices(range.start, range.end);

        int numberOfQueries = 0;
        for (List<DateRange> ranges : slices.values())
        {
            numberOfQueries += ranges.size();
        }
        log.info("Range scanning with " + numberOfQueries + " queries (lower is better).");

        Combinator sum = new Summation();

        List<Iterator<Summary>> queries = new ArrayList<Iterator<Summary>>(numberOfQueries);
        DataStore ds = DataStoreFactory.get();

        Date min = null;
        Date max = null;

        for (DateRange.Type type : slices.keySet())
        {
            List<DateRange> ranges = slices.get(type);
            for (DateRange slice : ranges)
            {
                if (min == null || slice.start.before(min))
                {
                    min = slice.start;
                }
                if (max == null || slice.end.after(max))
                {
                    max = slice.end;
                }

                queries.add(ds.find(entityId, type, slice.start, slice.end));
            }
        }

        Map<String, Object> results = MetricUtils.summarize(queries.toArray(new Iterator[0]));
        Map<String, Object> data = new HashMap<String, Object>();

        for (String key : results.keySet())
        {
            for (String field : combine)
            {
                if (key.matches(field))
                {
                    data.put("result", sum.combine(results.get(key), data.get("result")));
                }
            }
        }

        data.put("__start__", min);
        data.put("__end__", max);

        return data;
    }

    public Object range(String entityId, String field, DateRange range) throws IOException
    {
        QueryOptimizer optimizer = new QueryOptimizer();
        Map<DateRange.Type,  List<DateRange>> slices = optimizer.optimalSlices(range.start, range.end);

        int numberOfQueries = 0;
        for (List<DateRange> ranges : slices.values())
        {
            numberOfQueries += ranges.size();
        }
        log.info("Range scanning with " + numberOfQueries + " queries (lower is better).");

        List<Iterator<Summary>> queries = new ArrayList<Iterator<Summary>>(numberOfQueries);
        DataStore ds = DataStoreFactory.get();

        Date min = null;
        Date max = null;

        for (DateRange.Type type : slices.keySet())
        {
            List<DateRange> ranges = slices.get(type);
            for (DateRange slice : ranges)
            {
                if (min == null || slice.start.before(min))
                {
                    min = slice.start;
                }
                if (max == null || slice.end.after(max))
                {
                    max = slice.end;
                }

                queries.add(ds.find(entityId, type, slice.start, slice.end));
            }
        }

        Map<String, Object> results = MetricUtils.summarize(queries.toArray(new Iterator[0]));
        Map<String, Object> data = new HashMap<String, Object>();

        for (String key : results.keySet())
        {
            if (key.matches(field))
            {
                data.put(key, results.get(key));
            }
        }

        data.put("__start__", min);
        data.put("__end__", max);

        return data;
    }

    public Map<String, Object> range(String entityId, DateRange range) throws IOException
    {
        QueryOptimizer optimizer = new QueryOptimizer();
        Map<DateRange.Type,  List<DateRange>> slices = optimizer.optimalSlices(range.start, range.end);

        int numberOfQueries = 0;
        for (List<DateRange> ranges : slices.values())
        {
            numberOfQueries += ranges.size();
        }
        log.info("Range scanning with " + numberOfQueries + " queries (lower is better).");

        List<Iterator<Summary>> queries = new ArrayList<Iterator<Summary>>(numberOfQueries);
        DataStore ds = DataStoreFactory.get();

        Date min = null;
        Date max = null;

        for (DateRange.Type type : slices.keySet())
        {
            List<DateRange> ranges = slices.get(type);
            for (DateRange slice : ranges)
            {
                if (min == null || slice.start.before(min))
                {
                    min = slice.start;
                }
                if (max == null || slice.end.after(max))
                {
                    max = slice.end;
                }
                
                queries.add(ds.find(entityId, type, slice.start, slice.end));
            }
        }

        Map<String, Object> results = MetricUtils.summarize(queries.toArray(new Iterator[0]));

        results.put("__start__", min);
        results.put("__end__", max);

        return results;
    }

    public List<Object> series(String entityId, DateRange.Type type, String[] combine, DateRange range) throws IOException
    {
        DataStore ds = DataStoreFactory.get();

        // TODO: Some way of making this a lazy eval type of list so we don't
        // suck up memory unless we really have to?
        List<Object> list = new ArrayList<Object>();
        Iterator<Summary> iter = ds.find(entityId, type, range.start, range.end);
        Combinator sum = new Summation();

        while (iter.hasNext())
        {
            Summary summary = iter.next();
            Map<String, Object> data = new HashMap<String, Object>(3);

            DateRange slice = DateRange.create(type, new Date(summary.getTimestamp()));

            for (String key : summary.getValues().keySet())
            {
                for (String field : combine)
                {
                    if (key.matches(field))
                    {
                        Object value = summary.getValues().get(key);
                        if (value != null)
                        {
                            Object combined = sum.combine(value, data.get("result"));
                            data.put("result", combined);
                        }
                    }
                }
            }

            if (data.size() != 0)
            {
                data.put("__start__", slice.start);
                data.put("__end__", slice.end);
                list.add(data);
            }
        }

        return list;
    }

    public List<Object> series(String entityId, DateRange.Type type, String field, DateRange range) throws IOException
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

            for (String key : summary.getValues().keySet())
            {
                if (key.matches(field))
                {
                    Object value = summary.getValues().get(key);
                    if (value != null)
                    {
                        data.put(field, value);
                        list.add(data);
                    }
                }
            }
            
            data.put("__start__", slice.start);
            data.put("__end__", slice.end);
        }

        return list;
    }

    public List<Map<String, Object>> series(String entityId, DateRange.Type type, DateRange range) throws IOException
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
            data.put("__start__", slice.start);
            data.put("__end__", slice.end);

            data.put("metrics", summary.getValues());
            list.add(data);
        }

        return list;
    }

    public Object get(String entityId, DateRange.Type type, String[] combine, DateRange range) throws IOException
    {
        DataStore ds = DataStoreFactory.get();

        Iterator<Summary> best = ds.find(entityId, type, range.start, range.end);

        Map<String, Object> results = MetricUtils.summarize(best);

        Combinator sum = new Summation();

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("__start__", range.start);
        data.put("__end__", range.end);
        data.put("result", 0);

        for (String key : results.keySet())
        {
            for (String field : combine)
            {
                if (key.matches(field))
                {
                    Object combined = sum.combine(data.get("result"), results.get(key));
                    data.put("result", combined);
                }
            }
        }

        return data;
    }
    
    public Object get(String entityId, DateRange.Type type, String field, DateRange range) throws IOException
    {
        DataStore ds = DataStoreFactory.get();

        Iterator<Summary> best = ds.find(entityId, type, range.start, range.end);

        Map<String, Object> results = MetricUtils.summarize(best);

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("__start__", range.start);
        data.put("__end__", range.end);

        for (String key : results.keySet())
        {
            if (key.matches(field))
            {
                data.put(key, results.get(key));
            }
        }

        return data;
    }

    public Map<String, Object> get(String entityId, DateRange.Type type, DateRange range) throws IOException
    {
        DataStore ds = DataStoreFactory.get();

        Iterator<Summary> best = ds.find(entityId, type, range.start, range.end);
        Map<String, Object> data = MetricUtils.summarize(best);
        data.put("__start__", range.start);
        data.put("__end__", range.end);

        return data;
    }
}
