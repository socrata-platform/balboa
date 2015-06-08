package com.socrata.balboa.agent;

import com.blist.metrics.impl.queue.MetricJmsQueue;
import com.socrata.balboa.agent.util.FileUtils;
import com.socrata.balboa.metrics.Metric;
import com.socrata.metrics.Fluff;
import com.socrata.metrics.MetricIdPart;
import com.socrata.metrics.MetricQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

public class MetricConsumer implements Runnable {


    private static final Logger log = LoggerFactory.getLogger(MetricConsumer.class);
    private static final String TIMESTAMP = "timestamp";
    private static final String ENTITY_ID = "entityId";
    private static final String NAME = "name";
    private static final String VALUE = "value";
    private static final String RECORD_TYPE = "type";

    private static final List<String> fields = Arrays.asList(TIMESTAMP,
            ENTITY_ID, NAME, VALUE, RECORD_TYPE);

    private final File directory;
    private final long sleepTime;
    private final String amqServer, amqQueue;

    /**
     * Single threaded Metric Consumer.
     */
    public MetricConsumer(File directory,
                          long sleepTime,
                          String amqServer,
                          String amqQueue) {
        if (directory == null || !directory.isDirectory())
            throw new IllegalArgumentException("Illegal Data directory " + directory);
        if (sleepTime <= 0)
            throw new IllegalArgumentException("Illegal Argument time " + sleepTime);
        if (amqServer == null)
            throw new NullPointerException("ActiveMQ Server cannot be null");
        if (amqQueue == null)
            throw new NullPointerException("ActiveMQ Queue cannot be null");
        this.directory = directory;
        this.sleepTime = sleepTime;
        this.amqServer = amqServer;
        this.amqQueue = amqQueue;
    }

    @Override
    public void run() {
        try {
            while (true) {
                // Process files within a single level of the data directory.
                File[] otherNamespaces = this.directory.listFiles(FileUtils.isDirectory);
                if (otherNamespaces == null) otherNamespaces = new File[0];
                List<File> namespaces = new ArrayList<>(Arrays.asList(otherNamespaces));

                namespaces.add(this.directory); // Also process any files in the root directory.
                int recordsProcessed = 0;
                long start = System.currentTimeMillis();
                for (File namespace : namespaces) {
                    recordsProcessed += processNamespace(namespace);
                }
                long processingTime = System.currentTimeMillis() - start;
                log.info("Processed " + recordsProcessed + " in " + processingTime + "ms");

                // TODO:
                // What we really want is a throttler here to even out the metric injection
                // during high loads, at the expense of metric data showing up later.
                // but.... I don't know what that eq should look like.

                // Wait until there is at least one more file to process
                Thread.sleep(this.sleepTime);
            }
        } catch (InterruptedException t) {
            // Could occur in the cases where we have an OOM
            log.info("Thread interrupted consumer stopping.");
        } catch (Exception e) {
            log.error("Exception thrown during metric file processing. Hard exit for all threads.", e);
            throw e;
        }
    }

    int processNamespace(File dir) {
        int recordsProcessed = 0;
        File[] fileArr = dir.listFiles(FileUtils.isFile);
        if (fileArr != null && fileArr.length > 1)
        {
            String[] filenameArr = new String[fileArr.length];
            for(int i = 0; i < fileArr.length; ++i) {
                filenameArr[i] = fileArr[i].getName();
            }
            Arrays.sort(filenameArr);
            // Doing this subList thing removes the file which is currently
            // being written to.
            for (String file : Arrays.asList(filenameArr).subList(0,
                    filenameArr.length - 1))
            {
                File metricsEventLog = new File(dir, file);

                MetricQueue q = MetricJmsQueue.getInstance(this.amqServer, this.amqQueue);
                List<Record> records;

                try {
                    records = processFile(metricsEventLog);

                    // Emit a metric about what the file size of the metric we consumed is.
                    try {
                        long timestamp = new Date().getTime();

                        MetricIdPart internalID = new MetricIdPart("metrics-internal");
                        q.create(internalID, new Fluff("metrics-consumer-files-consumed-size"), metricsEventLog.length(), timestamp, Metric.RecordType.AGGREGATE);
                        q.create(internalID, new Fluff("metrics-consumer-files-consumed-count"), 1, timestamp, Metric.RecordType.AGGREGATE);
                    } catch (Exception e) {
                        // When an error occurs it is not a terrible big deal because we are going to be calculating the rolling average anyways.
                        // Its more important that we don't emit a count metric if we couldn't emit a size metric.  Hence the placement of the call.
                        log.error("Unable to emit metrics about what type of file was consumed.", e);
                    }
                } catch (IOException e) {
                    log.error("Error reading records from " + metricsEventLog, e);
                    File broken = new File(metricsEventLog.getAbsolutePath() + FileUtils.BROKEN_FILE_EXTENSION);
                    if (!metricsEventLog.renameTo(broken)) {
                        log.warn("Unable to rename broken file " + metricsEventLog + " permissions issue?");
                    }
                    continue;
                }

                for (Record r : records)
                    q.create(new MetricIdPart(r.entityId), new Fluff(r.name), r.value.longValue(), r.timestamp, r.type);
                // Flush the file to JMS after every read

                // TODO I don't think we need this so commenting out the below line.
//                q.flushWriteBuffer();
                recordsProcessed += records.size();
                if(!metricsEventLog.delete())
                {
                    log.error("Unable to delete event log " + metricsEventLog + " - file may be read twice, which is bad.");
                }
            }
        }
        return recordsProcessed;
    }

    private static class Record
    {
        public final String entityId;
        public final String name;
        public final Number value;
        public final Metric.RecordType type;
        public final long timestamp;

        public Record(String entityId, String name, Number value, long timestamp, Metric.RecordType type)
        {
            this.entityId = entityId;
            this.name = name;
            this.value = value;
            this.timestamp = timestamp;
            this.type = type;
        }
    }

    private static final Pattern integerPattern = Pattern.compile("-?[0-9]+");

    private List<Record> processFile(File f) throws IOException
    {
        List<Record> results = new ArrayList<Record>();
        InputStream stream = new BufferedInputStream(new FileInputStream(f));
        try
        {
            while (true)
            {
                Map<String, String> record = grovel(stream);
                if (record == null)
                    break;

                String rawValue = record.get(VALUE);
                Number value = null;
                if (!rawValue.equals("null"))
                {
                    try {
                        if (integerPattern.matcher(rawValue).matches())
                            value = Long.valueOf(rawValue);
                        else
                            value = Double.valueOf(rawValue);
                    } catch (NumberFormatException e) {
                        log.error("NumberFormatException reading metric from record: " + record.toString(), e);
                        continue;
                    }
                }

                results.add(new Record(record.get(ENTITY_ID), record.get(NAME),
                        value, Long.parseLong(record.get(TIMESTAMP)),
                        Metric.RecordType.valueOf(record.get(RECORD_TYPE).toUpperCase())));
            }
        }
        finally
        {
            stream.close();
        }
        return results;
    }

    // returns null on EOF
    private Map<String, String> grovel(InputStream stream) throws IOException
    {
        // First we have to find the start-of-record (a 0xff byte).
        // It *should* be the very first byte we're looking at.
        while (true)
        {
            int b = stream.read();
            if (b == 0xff)
                break;
            if (b == -1)
                return null;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        outer: while (true)
        {
            Map<String, String> record = new HashMap<String, String>();
            for (String field : fields)
            {
                baos.reset();

                while (true)
                {
                    int b = stream.read();
                    if (b == -1)
                        return null; // last record truncated

                    if (b == 0xff)
                    {
                        // ack, found an incomplete record!
                        log.warn("Found an incomplete record; complete fields were " + record);

                        throw new IOException("Unexpected 0xFF field in file. Refusing to continue to process since our file is almost certainly corrupt.");
                    }

                    if (b == 0xfe)
                        break;

                    baos.write(b);
                }

                String value = new String(baos.toByteArray(), "utf-8");
                record.put(field, value);
            }

            return record;
        }
    }
}
