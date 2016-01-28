package com.socrata.balboa.agent;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
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

/**
 * NOTE: Copied and Pasted from another existing project.
 * TODO: Deprecate this Concurrent Modification Exception prone class.
 */
public class MetricConsumer implements Runnable {

    /*
    * TODO: Remove all references to while true
    * TODO: Standardize Serialization.
     */

    private static final Logger log = LoggerFactory.getLogger(MetricConsumer.class);
    private static final String TIMESTAMP = "timestamp";
    private static final String ENTITY_ID = "entityId";
    private static final String NAME = "name";
    private static final String VALUE = "value";
    private static final String RECORD_TYPE = "type";

    private static final List<String> fields = Arrays.asList(TIMESTAMP,
            ENTITY_ID, NAME, VALUE, RECORD_TYPE);

    private final File directory;
    private final MetricQueue queue;

    /**
     * Single threaded Metric Consumer.
     */
    public MetricConsumer(File directory, MetricQueue queue) {
        if (directory == null || !directory.isDirectory())
            throw new IllegalArgumentException("Illegal Data directory " + directory);
        if (queue == null)
            throw new NullPointerException("Metric Queue cannot be null");
        this.directory = directory;
        this.queue = queue;
    }

    @Override
    public void run() {
        log.info("Starting " + this.getClass().getSimpleName());
        // Measure how long each process job is taking.
        final Timer.Context context = BalboaAgentMetrics.runtimeTimer().time();
        try {
            Set<File> namespaces = FileUtils.getDirectories(this.directory);
            int recordsProcessed = 0;
            long start = System.currentTimeMillis();
            for (File namespace : namespaces) {
                recordsProcessed += processNamespace(namespace);
            }
            long processingTime = System.currentTimeMillis() - start;
            log.info("Processed " + recordsProcessed + " in " + processingTime + "ms");
            BalboaAgentMetrics.metricsEmittedCount().inc(recordsProcessed);
            BalboaAgentMetrics.metricsEmittedMeter().mark(recordsProcessed);
        } finally {
            context.stop();
        }
    }

    private int processNamespace(File dir) {
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
            for (String file : Arrays.asList(filenameArr).subList(0, filenameArr.length - 1)) {
                File metricsEventLog = new File(dir, file);

                List<Record> records;

                try {
                    records = processFile(metricsEventLog);
                } catch (IOException e) {
                    log.error("Error reading records from " + metricsEventLog, e);
                    BalboaAgentMetrics.metricsProcessingFailureCounter().inc();
                    File broken = new File(metricsEventLog.getAbsolutePath() + FileUtils.BROKEN_FILE_EXTENSION);
                    if (!metricsEventLog.renameTo(broken)) {
                        log.warn("Unable to rename broken file " + metricsEventLog + " permissions issue?");
                        BalboaAgentMetrics.renameBrokenFileFailureCounter().inc();
                    }
                    continue;
                }

                for (Record r : records)
                    queue.create(new MetricIdPart(r.entityId), new Fluff(r.name), r.value.longValue(), r.timestamp, r.type);
                // Flush the file to JMS after every read

                // TODO I don't think we need this so commenting out the below line.
//                q.flushWriteBuffer();
                recordsProcessed += records.size();
                if(!metricsEventLog.delete())
                {
                    log.error("Unable to delete event log " + metricsEventLog + " - file may be read twice, which is bad.");
                    BalboaAgentMetrics.deleteEventFailureCounter().inc();
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

    private List<Record> processFile(File f) throws IOException {
        String filePath = f.getAbsolutePath();
        log.info("Processing file {}", filePath);
        List<Record> results = new ArrayList<Record>();
        InputStream stream = new BufferedInputStream(new FileInputStream(f));
        try {
            while (true) {
                Map<String, String> record = grovel(stream);
                if (record == null) {
                    log.error("Unable to process records from {}", filePath);
                    break;
                }

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
                        BalboaAgentMetrics.errorInvalidValueCounter().inc();
                        continue;
                    }
                }

                results.add(new Record(record.get(ENTITY_ID), record.get(NAME),
                        value, Long.parseLong(record.get(TIMESTAMP)),
                        Metric.RecordType.valueOf(record.get(RECORD_TYPE).toUpperCase())));
            }
        } finally {
            log.info("Completed (possibly with errors) file {}", filePath);
            // Perculate the exception up the call stack.... ugh.
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
                        BalboaAgentMetrics.incompleteFieldCounter().inc();
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
