package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.data.impl.CassandraDataStore;

import java.io.IOException;
import java.util.*;

public class QueryOptimizer
{
    DateRange.Period lessGranular(DateRange.Period current)
    {
        List<DateRange.Period> types;
        try
        {
            types = Configuration.get().getSupportedTypes();
        }
        catch (IOException e)
        {
            throw new CassandraDataStore.CassandraQueryException("Unable to load configuration for some reason.", e);
        }
        
        current = current.lessGranular();
        while (current != null && !types.contains(current))
        {
            current = current.lessGranular();
        }

        return current;
    }

    void optimize(Date start, Date end, DateRange.Period type, Map<DateRange.Period, Set<DateRange>> results)
    {
        // Check to see if you're finishing pinching...
        if (type == null || type == DateRange.Period.FOREVER || start.after(end))
        {
            return;
        }

        DateRange.Period nextPeriod = lessGranular(type);

        Set<DateRange> tier = new HashSet<DateRange>();
        results.put(type, tier);

        // Align the dates along the current border.
        start = DateRange.create(type, start).start;
        end = DateRange.create(type, end).end;

        Date nextStart = start;
        Date nextEnd = end;

        if (nextPeriod == null)
        {
            DateRange remaining = new DateRange(start, end);
            tier.add(remaining);
        }
        else
        {
            DateRange startSlice = null;
            if (!DateRange.liesOnBoundary(start, nextPeriod))
            {
                startSlice = new DateRange(
                        start,
                        Collections.min(Arrays.asList(DateRange.create(nextPeriod, start).end, end))
                );

                nextStart = new Date(startSlice.end.getTime() + 1);
            }

            DateRange endSlice = null;
            if (!DateRange.liesOnBoundary(end, nextPeriod))
            {
                endSlice = new DateRange(
                        Collections.max(Arrays.asList(DateRange.create(nextPeriod, end).start, start)),
                        end
                );
                
                nextEnd = new Date(endSlice.start.getTime() - 1);
            }

            if (startSlice != null && endSlice != null && startSlice.end.getTime() == (endSlice.start.getTime() - 1))
            {
                tier.add(new DateRange(startSlice.start, endSlice.end));
            }
            else
            {
                if (startSlice != null)
                {
                    tier.add(startSlice);
                }

                if (endSlice != null)
                {
                    tier.add(endSlice);
                }
            }

            optimize(nextStart, nextEnd, nextPeriod, results);
        }
    }

    public Map<DateRange.Period, Set<DateRange>> optimalSlices(Date start, Date end)
    {
        List<DateRange.Period> types;
        try
        {
            types = Configuration.get().getSupportedTypes();
        }
        catch (IOException e)
        {
            throw new CassandraDataStore.CassandraQueryException("Unable to load configuration for some reason.", e);
        }

        Map<DateRange.Period,  Set<DateRange>> optimized = new HashMap<DateRange.Period,  Set<DateRange>>();
        optimize(start, end, DateRange.Period.mostGranular(types), optimized);

        return optimized;
    }
}
