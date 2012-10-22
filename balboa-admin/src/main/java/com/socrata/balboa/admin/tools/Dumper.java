package com.socrata.balboa.admin.tools;

import au.com.bytecode.opencsv.CSVWriter;
import com.socrata.balboa.metrics.Metric;
import com.socrata.balboa.metrics.Timeslice;
import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.data.*;

import java.io.IOException;
import java.util.*;

public class Dumper
{
    CSVWriter writer;

    public Dumper(CSVWriter writer)
    {
        this.writer = writer;
    }

    int output(String entityId, Iterator<Timeslice> iter, CSVWriter writer)
    {
        Set<String> metricNames = new HashSet<String>();
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
        Period mostGranular = Period.mostGranular(Configuration.get().getSupportedPeriods());
        Period leastGranular = Period.leastGranular(Configuration.get().getSupportedPeriods());
        Date epoch = new Date(0);
        Date cutoff = DateRange.create(leastGranular, new Date()).end;
        DataStore ds = DataStoreFactory.get();
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
                    new IllegalStateException("Did not get the right number of metrics for entity " + entityId + " expected: " + expected + " got: " + got);
                }
            }
        }
    }

    public void dump(List<String> filters) throws IOException
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
        while (entities.hasNext())
        {
            String entityId = entities.next();
            dumpOnly(entityId);
        }
    }
}
