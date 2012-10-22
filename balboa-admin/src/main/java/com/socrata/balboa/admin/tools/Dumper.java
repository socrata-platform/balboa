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

    void output(String entityId, Iterator<Timeslice> iter, CSVWriter writer)
    {
        while (iter.hasNext())
        {
            Timeslice slice = iter.next();
            long timestamp = slice.getStart();

            for (Map.Entry<String, Metric> metric : slice.getMetrics().entrySet())
            {
                writer.writeNext(new String[] {
                        entityId,
                        Long.toString(timestamp),
                        metric.getKey(),
                        metric.getValue().getType().toString(),
                        metric.getValue().getValue().toString()
                });
            }
        }
    }

    public void dumpOnly(String entityId) throws IOException {
        Period mostGranular = Period.mostGranular(Configuration.get().getSupportedPeriods());
        Date epoch = new Date(0);
        Date cutoff = DateRange.create(mostGranular, new Date()).start;
        DataStore ds = DataStoreFactory.get();
        Iterator<Timeslice> slices = ds.slices(entityId, mostGranular, epoch, cutoff);
        output(entityId, slices, writer);
    }

    public void dump(List<String> filters) throws IOException
    {
        Period mostGranular = Period.mostGranular(Configuration.get().getSupportedPeriods());

        Date epoch = new Date(0);
        Date cutoff = DateRange.create(mostGranular, new Date()).start;

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
            Iterator<Timeslice> slices = ds.slices(entityId, mostGranular, epoch, cutoff);
            output(entityId, slices, writer);
        }
    }
}
