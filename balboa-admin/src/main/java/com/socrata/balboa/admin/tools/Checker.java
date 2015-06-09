package com.socrata.balboa.admin.tools;

import com.socrata.balboa.metrics.Metric;
import com.socrata.balboa.metrics.Metrics;
import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.data.*;

import java.io.IOException;
import java.util.*;

public class Checker
{
    public Period lessGranular(Period period) throws IOException
    {
        List<Period> periods = Configuration.get().getSupportedPeriods();
        period = period.lessGranular();
        while (period != null && !periods.contains(period))
        {
            period = period.lessGranular();
        }

        return period;
    }

    public Map<String, List<Number>> difference(String entityId, Date start, Period period) throws IOException
    {
        DataStore ds = DataStoreFactory.get();

        DateRange range = DateRange.create(lessGranular(period), start);
        Metrics moreGranularResults = Metrics.summarize(ds.find(entityId, period, range.getStart(), range.getEnd()));
        Metrics lessGranularResults = Metrics.summarize(ds.find(entityId, lessGranular(period), start));

        Map<String, List<Number>> results = new HashMap<String, List<Number>>();
        Set<Map.Entry<String, Metric>> diff =  moreGranularResults.difference(lessGranularResults);

        for (Map.Entry<String, Metric> entry : diff)
        {
            if (!results.containsKey(entry.getKey()))
            {
                results.put(entry.getKey(), new ArrayList<Number>(2));
            }

            results.get(entry.getKey()).add(entry.getValue().getValue());
        }

        return results;
    }

    public void valid()
    {
        System.out.println("[\033[92mvalid\033[0m]");
    }

    public void error()
    {
        System.err.println("[\033[91merror\033[0m]");
    }

    public void check(List<String> filters) throws IOException
    {
        DataStore ds = DataStoreFactory.get();
        Iterator<String> entities;

        if (filters.size() > 0)
        {
            List<Iterator<String>> iters = new ArrayList<Iterator<String>>(filters.size());
            for (String filter : filters)
            {
                iters.add(ds.entities(filter));
            }
            entities = new CompoundIterator<String>(iters.toArray(new Iterator[] {}));
        }
        else
        {
            entities = ds.entities();
        }

        List<Period> periods = new ArrayList<Period>(Configuration.get().getSupportedPeriods());
        Period leastGranular = Period.leastGranular(periods);

        // Remove the last (least granular) item from the list so that we don't
        // try to validate (least granular -> null).
        periods.remove(periods.size() - 1);

        while (entities.hasNext())
        {
            boolean errored = false;
            String entity = entities.next();

            Date current = new Date(0);
            Date cutoff = new Date();

            System.out.print(entity + " ... ");
            System.out.flush();

            while (current.before(cutoff))
            {
                for (Period period : periods)
                {
                    Map<String, List<Number>> diff = difference(entity, current, period);

                    if (diff.size() > 0)
                    {
                        if (!errored) {
                            error();
                            errored = true;
                        }

                        DateRange range = DateRange.create(lessGranular(period), current);
                        for (Map.Entry<String, List<Number>> entry : diff.entrySet())
                        {
                            String first, second = null;
                            if (entry.getValue().size() > 1)
                            {
                                first = entry.getValue().get(0).toString();
                                second = entry.getValue().get(1).toString();
                            }
                            else
                            {
                                // Sometimes
                                first = entry.getValue().get(0).toString();
                            }

                            String problem = String.format(
                                    "\t%s[%s] (%s -> %s %s/%s): %s != %s",
                                    entity,
                                    entry.getKey(),
                                    range.getStart().toString(),
                                    range.getEnd().toString(),
                                    period.toString(),
                                    lessGranular(period).toString(),
                                    first,
                                    second
                            );

                            System.err.println(problem);
                        }
                    }
                }

                current = new Date(DateRange.create(leastGranular, current).getEnd().getTime() + 1);
            }

            if (!errored) valid();
        }
    }
}
