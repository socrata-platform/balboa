package com.socrata.balboa.server;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.measurements.combining.Combinator;
import com.socrata.balboa.metrics.measurements.combining.Summation;
import com.socrata.balboa.metrics.utils.MetricUtils;
import com.socrata.balboa.server.exceptions.InternalException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.*;

public class MetricsService
{
    private static Log log = LogFactory.getLog(MetricsService.class);

    public Map<DateRange.Type, List<DateRange>> optimalSlices(Date start, Date end)
    {
        Map<DateRange.Type, List<DateRange>> ranges = new HashMap<DateRange.Type, List<DateRange>>();

        List<DateRange.Type> types;
        try
        {
            types = Configuration.get().getSupportedTypes();
        }
        catch (IOException e)
        {
            throw new InternalException("Unable to load configuration for some reason.", e);
        }

        DateRange.Type mostGranular = DateRange.Type.mostGranular(types);
        DateRange.Type leastGranular = DateRange.Type.leastGranular(types);

        // Align our base unit along the most granular type that is configured
        start = DateRange.create(mostGranular, start).start;
        end = DateRange.create(mostGranular, end).end;

        DateRange.Type current = mostGranular;
        while (current != null && current != DateRange.Type.FOREVER)
        {
            if (start.getTime() + 1 < end.getTime())
            {
                // Create a list for this tier.
                List<DateRange> tierRanges = new ArrayList<DateRange>();
                ranges.put(current, tierRanges);

                tierRanges.addAll(findToBoundary(start, end, current));

                Date startUpgradeTime = DateRange.create(current.lessGranular(), start).end;
                Date endUpgradeTime = DateRange.create(current.lessGranular(), end).start;

                if (current == leastGranular)
                {
                    // If we've done our leas gran summaries, that's the best we
                    // can do, so just get out of here. (woop woop woop)
                    break;
                }

                // Move our iterator dates a little beyond the boundary, unless
                // this particular boundary is the same as the boundary of it's
                // less granular parent. We don't want to push past the barrier
                // if the parent is the same, because instead we want to use
                // the parents summary and pushing past will force us to
                // continue down a non-optimal path.
                Date parentStartUpgradeTime = DateRange.create(current.lessGranular().lessGranular(), start).end;
                if (startUpgradeTime.before(parentStartUpgradeTime))
                {
                    start = new Date(startUpgradeTime.getTime() + 1);
                }
                else
                {
                    start = startUpgradeTime;
                }

                Date parentEndUpgradeTime = DateRange.create(current.lessGranular().lessGranular(), end).start;
                if (endUpgradeTime.after(parentEndUpgradeTime))
                {
                    end = new Date(endUpgradeTime.getTime() - 1);
                }
                else
                {
                    end = endUpgradeTime;
                }

                current = current.lessGranular();
                while (current != null && !types.contains(current))
                {
                    current = current.lessGranular();
                }
            }
            else
            {
                // Start boundary is not before the end boundary, so we've
                // covered everything the best we can and don't need to
                // continue.
                break;
            }
        }

        return ranges;
    }

    private List<DateRange> findToBoundary(Date start, Date end, DateRange.Type type)
    {
        List<DateRange> ranges = new ArrayList<DateRange>();

        Date endMin = null;
        Date endMax = null;

        Date startMin = null;
        Date startMax = null;

        Date startUpgradeTime = DateRange.create(type.lessGranular(), start).end;
        Date endUpgradeTime = DateRange.create(type.lessGranular(), end).start;

        while (true)
        {
            if (end.after(endUpgradeTime) && end.after(start))
            {
                DateRange range = DateRange.create(type, end);

                if (endMin == null || range.start.before(endMin))
                {
                    endMin = range.start;
                }

                if (endMax == null || range.end.after(endMax))
                {
                    endMax = range.end;
                }

                // Move to the next discrete segment.
                end = new Date(range.start.getTime() - 1);
            }

            if (start.before(startUpgradeTime) && start.before(end))
            {
                DateRange range = DateRange.create(type, start);

                if (startMin == null || range.start.before(startMin))
                {
                    startMin = range.start;
                }

                if (startMax == null || range.end.after(startMax))
                {
                    startMax = range.end;
                }

                // Move to the next discrete segment.
                start = new Date(range.end.getTime() + 1);
            }

            if
            (
                end.before(start) || start.after(end) ||
                (
                        (start.after(startUpgradeTime) || start.equals(startUpgradeTime)) && 
                        (end.before(endUpgradeTime) || end.equals(endUpgradeTime))
                )
            )
            {
                // Hopefully at this point we're done :-)
                break;
            }
        }

        if
        (
            (startMax != null && endMin != null) &&
            (startMax.equals(endMin) || (startMax.getTime() + 1 == endMin.getTime()))
        )
        {
            // If the ranges are adjacent, combine them.
            ranges.add(new DateRange(startMin, endMax));
        }
        else
        {
            // If the ranges aren't adjacent, add both of them separately.
            if (startMin != null && startMax != null)
            {
                ranges.add(new DateRange(startMin, startMax));
            }

            if (endMin != null && endMax != null)
            {
                ranges.add(new DateRange(endMin, endMax));
            }
        }

        return ranges;
    }

    public Object range(String entityId, String[] combine, DateRange range) throws IOException
    {
        Map<DateRange.Type,  List<DateRange>> slices = optimalSlices(range.start, range.end);

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
        Map<DateRange.Type,  List<DateRange>> slices = optimalSlices(range.start, range.end);

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
        Map<DateRange.Type,  List<DateRange>> slices = optimalSlices(range.start, range.end);

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
