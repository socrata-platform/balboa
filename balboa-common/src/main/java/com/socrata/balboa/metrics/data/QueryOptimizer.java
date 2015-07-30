package com.socrata.balboa.metrics.data;

import com.socrata.balboa.common.config.Configuration;

import java.io.IOException;
import java.util.*;

public class QueryOptimizer {
    Period lessGranular(Period current) {
        List<Period> types;
        try {
            types = Configuration.get().getSupportedPeriods();
        } catch (IOException e) {
            throw new QueryException("Unable to load configuration for some reason.", e);
        }

        current = current.lessGranular();
        while (current != null && !types.contains(current)) {
            current = current.lessGranular();
        }

        return current;
    }

    void optimize(Date start, Date end, Period type, Map<Period, Set<DateRange>> results) {
        // Check to see if you're finishing pinching...
        if (type == null || type == Period.FOREVER || start.after(end)) {
            return;
        }

        Period nextPeriod = lessGranular(type);

        Set<DateRange> tier = new HashSet<DateRange>();
        results.put(type, tier);

        // Align the dates along the current border.
        start = DateRange.create(type, start).getStart();
        end = DateRange.create(type, end).getEnd();

        Date nextStart = start;
        Date nextEnd = end;

        if (nextPeriod == null) {
            DateRange remaining = new DateRange(start, end);
            tier.add(remaining);
        } else {
            DateRange startSlice = null;
            if (!DateRange.liesOnBoundary(start, nextPeriod)) {
                startSlice = new DateRange(
                        start,
                        Collections.min(Arrays.asList(DateRange.create(nextPeriod, start).getEnd(), end))
                );

                nextStart = new Date(startSlice.getEnd().getTime() + 1);
            }

            DateRange endSlice = null;
            if (!DateRange.liesOnBoundary(end, nextPeriod)) {
                endSlice = new DateRange(
                        Collections.max(Arrays.asList(DateRange.create(nextPeriod, end).getStart(), start)),
                        end
                );

                nextEnd = new Date(endSlice.getStart().getTime() - 1);
            }

            if (startSlice != null && endSlice != null && startSlice.getEnd().getTime() == (endSlice.getStart().getTime() - 1)) {
                tier.add(new DateRange(startSlice.getStart(), endSlice.getEnd()));
            } else {
                if (startSlice != null) {
                    tier.add(startSlice);
                }

                if (endSlice != null) {
                    tier.add(endSlice);
                }
            }

            optimize(nextStart, nextEnd, nextPeriod, results);
        }
    }

    public Map<Period, Set<DateRange>> optimalSlices(Date start, Date end) {
        List<Period> types;
        try {
            types = Configuration.get().getSupportedPeriods();
        } catch (IOException e) {
            throw new QueryException("Unable to load configuration for some reason.", e);
        }

        Map<Period, Set<DateRange>> optimized = new HashMap<Period, Set<DateRange>>();
        optimize(start, end, Period.mostGranular(types), optimized);

        return optimized;
    }
}
