package com.socrata.balboa.admin.tools;

import au.com.bytecode.opencsv.CSVWriter;
import com.socrata.balboa.metrics.Metric;
import com.socrata.balboa.metrics.Timeslice;
import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.data.Period;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

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

    public void dump() throws IOException
    {
        Period mostGranular = Period.mostGranular(Configuration.get().getSupportedPeriods());

        Date epoch = new Date(0);
        Date cutoff = DateRange.create(mostGranular, new Date()).start;

        DataStore ds = DataStoreFactory.get();
        Iterator<String> entities = ds.entities();
        while (entities.hasNext())
        {
            String entityId = entities.next();
            Iterator<Timeslice> slices = ds.slices(entityId, mostGranular, epoch, cutoff);
            output(entityId, slices, writer);
        }
    }
}
