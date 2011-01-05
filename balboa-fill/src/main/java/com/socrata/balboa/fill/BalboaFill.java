package com.socrata.balboa.fill;

import au.com.bytecode.opencsv.CSVReader;
import com.socrata.balboa.metrics.Metric;
import com.socrata.balboa.metrics.Metrics;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class BalboaFill
{
    public static void usage()
    {
        System.err.println("java -jar balboa-fill file1 [file2...]");
    }

    public static void main(String[] args)
    {
        long startTime = System.currentTimeMillis();

        int count = 0;

        List<File> files = new ArrayList<File>(args.length);
        for (String arg : args)
        {
            File file = new File(arg);
            files.add(file);
        }

        DataStore ds = DataStoreFactory.get();
        for (File file : files)
        {
            System.out.println("Processing " + file.getPath());

            try
            {
                CSVReader reader = new CSVReader(new FileReader(file));
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

                    Metrics metrics = new Metrics();
                    metrics.put(metric, new Metric(type, value));

                    ds.persist(entityId, timestamp, metrics);
                }
            }
            catch (IOException e)
            {
                throw new IllegalArgumentException("Unable to parse/read " + file + ".", e);
            }
        }

        long totalTime = (System.currentTimeMillis() - startTime) / 1000;
        double avgInsertsPerSecond = count / totalTime / 1000;
        System.out.println("\nTotal time: " + totalTime + " (" + avgInsertsPerSecond + " rows/sec)");
    }
}
