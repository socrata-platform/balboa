package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.utils.MetricUtils;

import java.util.*;

/**
 * A simple datastore that uses an in memory hash instead of an external
 * service. Can be used for testing without having to launch a background
 * cassandra instance. This should never be used for anything but testing. This
 * is probably embarrassingly slow and horrible so just don't use it.
 *
 * p.s. I'm not thread safe either.
 */
public class MapDataStore implements DataStore
{
    static MapDataStore instance;

    public static MapDataStore getInstance()
    {
        if (instance == null)
        {
            instance = new MapDataStore();
        }

        return instance;
    }

    public Map<String, List<Summary>> data = new HashMap<String, List<Summary>>();

    /**
     * An iterator that filters out all summaries from a list that aren't within
     * the date range.
     */
    static class DateFilter implements Iterator<Summary>
    {
        DateRange range;
        Summary next = null;
        Iterator<Summary> internal;

        DateFilter(List<Summary> list, DateRange range)
        {
            this.internal = list.iterator();
            this.range = range;
        }

        @Override
        public boolean hasNext()
        {
            if (next != null)
            {
                return true;
            }

            if (!internal.hasNext())
            {
                return false;
            }
            else
            {
                while (internal.hasNext())
                {
                    Summary summary = internal.next();
                    if ((range.start.getTime() <= summary.getTimestamp()) &&
                        (summary.getTimestamp() <= range.end.getTime()))
                    {
                        // We're within the filtered time range.
                        next = summary;
                        break;
                    }
                }
            }

            if (next != null)
            {
                return true;
            }

            return false;
        }

        @Override
        public Summary next()
        {
            if (!hasNext())
            {
                throw new NoSuchElementException("There are no more summaries.");
            }
            else
            {
                Summary ret = next;
                next = null;
                return ret;
            }
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("Not supported.");
        }
    }
    
    @Override
    public Iterator<Summary> find(String entityId, Summary.Type type, Date date)
    {
        if (data.containsKey(entityId))
        {
            return new DateFilter(data.get(entityId), DateRange.create(type, date));
        }
        else
        {
            List<Summary> mock = new ArrayList<Summary>(0);
            return mock.iterator();
        }
    }

    @Override
    public Iterator<Summary> find(String entityId, Summary.Type type, Date start, Date end)
    {
        if (data.containsKey(entityId))
        {
            return new DateFilter(data.get(entityId), new DateRange(start, end));
        }
        else
        {
            List<Summary> mock = new ArrayList<Summary>(0);
            return mock.iterator();
        }
    }

    @Override
    public void persist(String entityId, Summary summary)
    {
        if (!data.containsKey(entityId))
        {
            data.put(entityId, new ArrayList<Summary>());
        }

        List<Summary> summaries = data.get(entityId);
        Iterator<Summary> iter = summaries.iterator();

        boolean merged = false;

        while (iter.hasNext())
        {
            Summary maybeTheSame = iter.next();

            if (maybeTheSame.getTimestamp() == summary.getTimestamp())
            {
                MetricUtils.merge(maybeTheSame.getValues(), summary.getValues());
                merged = true;
            }
        }

        if (!merged)
        {
            summaries.add(summary);
        }
    }

    public static void destroy()
    {
        instance = null;
    }
}
