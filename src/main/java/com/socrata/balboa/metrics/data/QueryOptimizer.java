package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.server.exceptions.InternalException;

import java.io.IOException;
import java.util.*;

public class QueryOptimizer
{
    public Map<DateRange.Period, List<DateRange>> optimalSlices(Date start, Date end)
    {
        Map<DateRange.Period, List<DateRange>> ranges = new HashMap<DateRange.Period, List<DateRange>>();

        List<DateRange.Period> periods;
        try
        {
            periods = Configuration.get().getSupportedTypes();
        }
        catch (IOException e)
        {
            throw new InternalException("Unable to load configuration for some reason.", e);
        }

        DateRange.Period mostGranular = DateRange.Period.mostGranular(periods);
        DateRange.Period leastGranular = DateRange.Period.leastGranular(periods);

        // Align our base unit along the most granular type that is configured
        start = DateRange.create(mostGranular, start).start;
        end = DateRange.create(mostGranular, end).end;

        DateRange.Period current = mostGranular;
        while (current != null && current != DateRange.Period.FOREVER)
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
                while (current != null && !periods.contains(current))
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

    static List<DateRange> findToBoundary(Date start, Date end, DateRange.Period period)
    {
        List<DateRange> ranges = new ArrayList<DateRange>();

        Date endMin = null;
        Date endMax = null;

        Date startMin = null;
        Date startMax = null;

        Date startUpgradeTime = DateRange.create(period.lessGranular(), start).end;
        Date endUpgradeTime = DateRange.create(period.lessGranular(), end).start;

        while (true)
        {
            if (end.after(endUpgradeTime) && end.after(start))
            {
                DateRange range = DateRange.create(period, end);

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
                DateRange range = DateRange.create(period, start);

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
}
