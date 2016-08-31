package com.socrata.balboa.metrics.data;

import java.util.*;

public class QueryOptimizer {

    private final List<Period> supportedPeriods;

    public QueryOptimizer(List<Period> supportedPeriods) {
        this.supportedPeriods = supportedPeriods;
    }

    Period lessGranular(Period current) {
        current = current.lessGranular();
        while (current != null && !supportedPeriods.contains(current)) {
            current = current.lessGranular();
        }

        return current;
    }

    /**
     * Determine the optimal way to cover the range [start, end] using available Period types.
     * @param type maximum granularity to consider
     * @param results a map to fill in, indicating which sub-ranges should be covered by which Period type.
     */
    void optimize(Date start, Date end, Period type, Map<Period, Set<DateRange>> results) {
        // Check to see if you're finishing pinching...
        if (type == null || type == Period.FOREVER || start.after(end)) {
            return;
        }

        Period nextPeriod = lessGranular(type);

        Set<DateRange> tier = new HashSet<>();
        results.put(type, tier);

        // Align the dates along the current border.
        start = DateRange.create(type, start).start;
        end = DateRange.create(type, end).end;

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
                        Collections.min(Arrays.asList(DateRange.create(nextPeriod, start).end, end))
                );

                nextStart = new Date(startSlice.end.getTime() + 1);
            }

            DateRange endSlice = null;
            if (!DateRange.liesOnBoundary(end, nextPeriod)) {
                endSlice = new DateRange(
                        Collections.max(Arrays.asList(DateRange.create(nextPeriod, end).start, start)),
                        end
                );

                nextEnd = new Date(endSlice.start.getTime() - 1);
            }

            if (startSlice != null && endSlice != null && startSlice.end.getTime() == (endSlice.start.getTime() - 1)) {
                tier.add(new DateRange(startSlice.start, endSlice.end));
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
        Map<Period, Set<DateRange>> optimized = new HashMap<>();
        optimize(start, end, Period.mostGranular(supportedPeriods), optimized);

        return optimized;
    }
}
