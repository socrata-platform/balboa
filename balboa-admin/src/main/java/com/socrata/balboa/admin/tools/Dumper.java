package com.socrata.balboa.admin.tools;

import au.com.bytecode.opencsv.CSVWriter;
import com.socrata.balboa.metrics.Metric;
import com.socrata.balboa.metrics.Timeslice;
import com.socrata.balboa.metrics.data.*;

import java.io.IOException;
import java.util.*;

public class Dumper
{
    CSVWriter writer;
    private final DataStoreFactory dataStoreFactory;
    private final List<Period> supportedPeriods;

    /**
     * @param supportedPeriods List of supported time periods ordered by increasing length
     */
    public Dumper(CSVWriter writer, DataStoreFactory dataStoreFactory, List<Period> supportedPeriods) {
        this.writer = writer;
        this.dataStoreFactory = dataStoreFactory;
        this.supportedPeriods = supportedPeriods;
    }

    int output(String entityId, Iterator<Timeslice> iter, CSVWriter writer)
    {
        Set<String> metricNames = new HashSet<>();
        while (iter.hasNext())
        {
            Timeslice slice = iter.next();
            long timestamp = slice.getStart();

            for (Map.Entry<String, Metric> metric : slice.getMetrics().entrySet())
            {
                metricNames.add(metric.getKey());
                writer.writeNext(new String[] {
                        entityId,
                        Long.toString(timestamp),
                        metric.getKey(),
                        metric.getValue().getType().toString(),
                        metric.getValue().getValue().toString()
                });
            }
        }
        return metricNames.size();
    }

    // Dump only a specific entity
    public void dumpOnly(String entityId) throws IOException {
        Period mostGranular = Period.mostGranular(supportedPeriods);
        Period leastGranular = Period.leastGranular(supportedPeriods);
        Date epoch = new Date(0);
        Date cutoff = DateRange.create(leastGranular, new Date()).end;
        DataStore ds = dataStoreFactory.get();
        Iterator<Timeslice> bigSlices = ds.slices(entityId, leastGranular, epoch, cutoff);
        while(bigSlices.hasNext()) {
            Timeslice current = bigSlices.next();
            int expected = current.getMetrics().size();
            if (expected > 0) {
                Date start = new Date(current.getStart());
                cutoff = new Date(current.getEnd());
                Iterator<Timeslice> slices = ds.slices(entityId, mostGranular, start, cutoff);
                int got = output(entityId, slices, writer);
                if (expected != got) {
                    throw new IllegalStateException(
                            "Did not get the right number of metrics for entity " + entityId +
                                    " expected: " + expected +
                                    " got: " + got
                    );
                }
            }
        }
    }

    public void dump(List<String> filters) throws IOException
    {
        DataStore ds = dataStoreFactory.get();
        Iterator<String> entities;
        if (filters.size() > 0)
        {
            List<Iterator<String>> iters = new ArrayList<>(filters.size());
            for (String filter : filters)
            {
                iters.add(ds.entities(filter));
            }
            entities = new CompoundIterator<>(iters);
        }
        else
        {
            entities = ds.entities();
        }
        while (entities.hasNext())
        {
            String entityId = entities.next();
            dumpOnly(entityId);
        }
    }
}
