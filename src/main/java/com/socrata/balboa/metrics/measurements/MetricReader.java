package com.socrata.balboa.metrics.measurements;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.Summary.Type;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.measurements.combining.Combinator;
import com.socrata.balboa.metrics.measurements.preprocessing.Preprocessor;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MetricReader
{
    Object summarize(String field, Type type, Preprocessor preprocessor, Combinator combinator, Iterator<Summary> iter) throws IOException
    {
        Object current = null;
        
        while (iter.hasNext())
        {
            Summary summary = iter.next();

            if (summary.getValues().containsKey(field))
            {
                String serializedValue = summary.getValues().get(field);

                Object value = preprocessor.toValue(serializedValue);
                current = combinator.combine(current, value);
            }
        }

        return current;
    }

    /**
     * Read the metrics for a given date range for a particular type. This method will recursively search for summaries
     * in different type column families underneath the current one if it can't find anything for the current.
     *
     * For example, say we have some metrics data that looks like the following:
     *
     * <code>
     * Metrics:
     *   realtime:
     *     2010-01-01 00:00:00:
     *       "rowId": {"views": 25}
     *     2010-01-02 00:00:00:
     *       "rowId": {"views": 10}
     * </code>
     *
     * The method call for a "month" type for the range 2010-01-01 -> 2010-02-01 doesn't contain any metric summaries in
     * the month column family. So it will recurse and call for each week, but none of the weeks have any summaries
     * either. The same happens for day, and so on until the "realtime" summaries are reached.
     *
     * As the stack unwinds, each summary slice up the chain with summarize itself and save that summary so the next
     * time it's accessed there's no need to recurse.
     */
    public Object read(String entityId, String field, Type type, DateRange range, DataStore ds, Preprocessor pre, Combinator com) throws IOException
    {
        // Start with the best possible summary and try to find it. If there's no items in the best possible summary,
        // then move on to the next best.
        Iterator<Summary> best = ds.find(entityId, type, range.start, range.end);

        if (best.hasNext())
        {
            // If there are already items in the best summary, then it's the best possible summary we could have. Return
            // that iterator and be done with it.
            return summarize(field, type, pre, com, best);
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

            Object current = null;
            while (piece.start.before(range.end))
            {
                // Add this piece of time to our list iterator.
                Object summary = read(entityId, field, type.nextBest(), piece, ds, pre, com);
                current = com.combine(current, summary);

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
            Map<String, String> values = new HashMap<String, String>();
            values.put(field, pre.toString(current));
            Summary summary = new Summary(type, range.start.getTime(), values);
            ds.persist(entityId, summary);

            // And at the end: We're done and we've got our metric.
            return current;
        }
        else
        {
            return null;
        }
    }
}
