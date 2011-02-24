package com.socrata.balboa.admin.tools;

import au.com.bytecode.opencsv.CSVReader;
import com.socrata.balboa.metrics.Metric;
import com.socrata.balboa.metrics.Metrics;
import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import com.socrata.balboa.metrics.data.DateRange;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Filler
{
    CSVReader reader;

    public Filler(CSVReader reader)
    {
        this.reader = reader;
    }

    public static class Buffer
    {
        Map<Long, Map<String, Metrics>> items;

        Buffer()
        {
            items = new HashMap<Long, Map<String, Metrics>>();
        }

        public synchronized void add(String entityId, long timestamp, String name, Metric metric) throws IOException
        {
            Date when = new Date(timestamp);
            DateRange.Period mostGranular = DateRange.Period.mostGranular(Configuration.get().getSupportedTypes());

            Metrics original = new Metrics();
            original.put(name, metric);

            long aligned = DateRange.create(mostGranular, when).start.getTime();

            if (!items.containsKey(aligned))
            {
                items.put(aligned, new HashMap<String, Metrics>());
            }

            if (!items.get(aligned).containsKey(entityId))
            {
                items.get(aligned).put(entityId, new Metrics());
            }

            Metrics metrics = items.get(aligned).get(entityId);

            metrics.merge(original);
        }

        public synchronized void flush() throws IOException
        {
            DataStore ds = DataStoreFactory.get();

            System.out.println("Flushing " + items.size() + " timeslices.");

            for (Map.Entry<Long, Map<String, Metrics>> entry : items.entrySet())
            {
                long timestamp = entry.getKey();

                for (Map.Entry<String, Metrics> metricsEntry : entry.getValue().entrySet())
                {
                    String entityId = metricsEntry.getKey();
                    Metrics saveable = metricsEntry.getValue();
                    ds.persist(entityId, timestamp, saveable);
                }
            }

            items.clear();
        }
    }

    static class Cleanser extends Thread
    {
        Buffer buffer;

        Cleanser(Buffer buffer)
        {
            this.buffer = buffer;
        }

        @Override
        public void run()
        {
            while (true)
            {
                try
                {
                    buffer.flush();
                }
                catch (IOException e)
                {
                    System.err.println("Unable to flush buffer for some reason.");
                }

                try
                {
                    Thread.sleep(10000);
                }
                catch (InterruptedException e)
                {
                    System.out.println("Buffer cleanser interrupted, stopping...");
                    return;
                }
            }
        }
    }

    public void fill() throws IOException
    {
        long startTime = System.currentTimeMillis();
        int count = 0;

        Buffer buffer = new Buffer();
        Cleanser cleanser = new Cleanser(buffer);
        cleanser.start();

        String [] line;
        while ((line = reader.readNext()) != null)
        {
            count += 1;

            if ((count % 1000) == 0)
            {
                System.out.print(".");
            }

            if ((count % 10000) == 0)
            {
                System.out.println(" " + count);
            }

            if (line.length != 5)
            {
                throw new IllegalArgumentException("Invalid csv format. There should be exactly 5 columns (entityId, timestamp, metric, record-type, value).");
            }

            String entityId = line[0];
            long timestamp = Long.parseLong(line[1]);
            String metric = line[2];
            Metric.RecordType type = Metric.RecordType.valueOf(line[3].toUpperCase());
            BigDecimal value = new BigDecimal(line[4]);

            buffer.add(entityId, timestamp, metric, new Metric(type, value));
        }

        System.out.println("Interrupting the buffer thread...");
        cleanser.interrupt();

        System.out.println("Purging the buffer...");
        buffer.flush();

        long totalTime = (System.currentTimeMillis() - startTime) / 1000;
        double avgInsertsPerSecond = count / (totalTime > 0 ? totalTime: 1) / 1000;
        System.out.println("\nTotal time: " + totalTime + " (" + avgInsertsPerSecond + " rows/sec)");
    }
}
