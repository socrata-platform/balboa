package com.socrata.balboa.server;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.utils.MetricUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.*;

public class MetricsService
{
    private static Log log = LogFactory.getLog(MetricsService.class);

    public Map<Summary.Type, List<DateRange>> optimalSlices(Date start, Date end)
    {
        Map<Summary.Type, List<DateRange>> ranges = new HashMap<Summary.Type, List<DateRange>>();

        // The best summary that we do is hourly, so use that as the base unit
        // and align on the hourly borders.
        start = DateRange.create(Summary.Type.HOURLY, start).start;
        end = DateRange.create(Summary.Type.HOURLY, end).end;

        Summary.Type current = Summary.Type.HOURLY;
        while (current != Summary.Type.FOREVER)
        {
            if (start.getTime() + 1 < end.getTime())
            {
                // Create a list for this tier.
                List<DateRange> tierRanges = new ArrayList<DateRange>();
                ranges.put(current, tierRanges);

                tierRanges.addAll(findToBoundary(start, end, current));

                Date startUpgradeTime = DateRange.create(current.lessGranular(), start).end;
                Date endUpgradeTime = DateRange.create(current.lessGranular(), end).start;

                if (current == Summary.Type.YEARLY)
                {
                    // If we've done our yearly summaries, that's the best we
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

    private List<DateRange> findToBoundary(Date start, Date end, Summary.Type type)
    {
        List<DateRange> ranges = new ArrayList<DateRange>();
        Date startUpgradeTime = DateRange.create(type.lessGranular(), start).end;
        Date endUpgradeTime = DateRange.create(type.lessGranular(), end).start;

        while (true)
        {
            if (end.after(endUpgradeTime) && end.after(start))
            {
                DateRange range = DateRange.create(type, end);
                ranges.add(range);

                // Move to the next discrete segment.
                end = new Date(range.start.getTime() - 1);
            }

            if (start.before(startUpgradeTime) && start.before(end))
            {
                DateRange range = DateRange.create(type, start);
                ranges.add(range);

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

        return ranges;
    }

    public Map<String, Object> range(String entityId, DateRange range) throws IOException
    {
        Map<Summary.Type,  List<DateRange>> slices = optimalSlices(range.start, range.end);

        int numberOfQueries = 0;
        for (List<DateRange> ranges : slices.values())
        {
            numberOfQueries += ranges.size();
        }
        log.info("Range scanning with " + numberOfQueries + " queries (lower is better).");

        List<Iterator<Summary>> queries = new ArrayList<Iterator<Summary>>(numberOfQueries);
        DataStore ds = DataStoreFactory.get();

        for (Summary.Type type : slices.keySet())
        {
            List<DateRange> ranges = slices.get(type);
            for (DateRange slice : ranges)
            {
                queries.add(ds.find(entityId, type, slice.start, slice.end));
            }
        }

        return MetricUtils.summarize(queries.toArray(new Iterator[0]));
    }

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
