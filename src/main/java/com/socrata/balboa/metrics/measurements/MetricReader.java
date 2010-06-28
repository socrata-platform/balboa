package com.socrata.balboa.metrics.measurements;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.Summary.Type;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.measurements.combining.Combinator;
import com.socrata.balboa.metrics.measurements.combining.sum;
import com.socrata.balboa.metrics.measurements.serialization.JsonSerializer;
import com.socrata.balboa.metrics.measurements.serialization.Serializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.*;

public class MetricReader
{
    private static Log log = LogFactory.getLog(MetricReader.class);

    Map<String, Object> summarize(Type type, Iterator<Summary> iter) throws IOException
    {
        log.debug("Summarizing configuration for type '" + type + "'.");

        int count = 0;
        
        Map<String, Object> results = new HashMap<String, Object>();

        Serializer serializer = new JsonSerializer();
        Combinator com = new sum();

        while (iter.hasNext())
        {
            Summary summary = iter.next();

            count += 1;

            for (String key : summary.getValues().keySet())
            {
                String serializedValue = summary.getValues().get(key);

                Object value = serializer.toValue(serializedValue);
                results.put(key, com.combine(results.get(key), value));
            }
        }

        log.debug("Summarized " + Integer.toString(count) + " columns.");

        return results;
    }

    Map<String, Object> merge(Map<String, Object> first, Map<String, Object> second)
    {
        Set<String> unionKeys = new HashSet<String>(first.keySet());
        unionKeys.addAll(second.keySet());

        sum com = new sum();

        for (String key : unionKeys)
        {
            first.put(key, com.combine((Number)first.get(key), (Number)second.get(key)));
        }
        return first;
    }

    Map<String, String> postprocess(Map<String, Object> map) throws IOException
    {
        Map<String, String> results = new HashMap<String, String>();
        Serializer serializer = new JsonSerializer();

        for (String key : map.keySet())
        {
            results.put(key, serializer.toString(map.get(key)));
        }
        return results;
    }

    public Object read(String entityId, String field, Type type, DateRange range, DataStore ds, boolean cache) throws IOException
    {
        Map<String, Object> results = read(entityId, type, range, ds, cache);

        return results.get(field);
    }

    public Object read(String entityId, String field, Type type, DateRange range, DataStore ds) throws IOException
    {
        return read(entityId, field, type, range, ds, false);
    }

    public Map<String, Object> read(String entityId, Type type, DateRange range, DataStore ds) throws IOException
    {
        return read(entityId, type, range, ds, true);
    }

    public Map<String, Object> read(String entityId, Type type, DateRange range, DataStore ds, boolean cache) throws IOException
    {
        // Start with the best possible summary and try to find it. If there's no items in the best possible summary,
        // then move on to the next best.
        Iterator<Summary> best = ds.find(entityId, type, range.start, range.end);

        if (best.hasNext())
        {
            // If there are already items in the best summary, then it's the best possible summary we could have.
            // Compute those results and be done with them.
            return summarize(type, best);
        }
        else if (type.nextBest() != null)
        {
            // Start slicing up our current date range into the pieces that fit into it, but are the next best type.
            DateRange piece;
            if (type.nextBest() == Type.REALTIME)
            {
                piece = range;
            }
            else
            {
                piece = DateRange.create(type.nextBest(), range.start);
            }

            Map<String, Object> current = new HashMap<String, Object>();
            while (piece.start.before(range.end))
            {
                Map<String, Object> summaries = read(entityId, type.nextBest(), piece, ds, cache);

                if (summaries != null)
                {
                    merge(current, summaries);
                }

                // Move to the next possible time slice and create that.
                Date after = new Date(piece.end.getTime() + 1);

                if (type.nextBest() == Type.REALTIME)
                {
                    break;
                }

                piece = DateRange.create(type.nextBest(), after);
            }

            // Summarize this time period permanently by writing it to the datastore. Subsequent reads will no longer
            // recurse to it's next best type summary.
            if (cache == true)
            {
                if (range.start.before(new Date()) && range.end.after(new Date()))
                {
                    log.debug("Range includes today so I can't summarize yet (patience, young one).");
                }
                else
                {
                    log.debug("Caching the summary range '" + range + "' in the type '" + type + "'");

                    Summary summary = new Summary(type, range.start.getTime(), postprocess(current));
                    ds.persist(entityId, summary);
                }
            }

            // And at the end: We're done and we've got our metric.
            return current;
        }
        else
        {
            return null;
        }
    }
}
