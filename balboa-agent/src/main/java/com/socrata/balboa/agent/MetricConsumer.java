package com.socrata.balboa.agent;

import com.codahale.metrics.Timer;
import com.socrata.balboa.agent.metrics.BalboaAgentMetrics;
import com.socrata.balboa.agent.util.FileUtils;
import com.socrata.balboa.metrics.Metric;
import com.socrata.metrics.Fluff;
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
public class MetricConsumer implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MetricConsumer.class);

    /*
     * TODO: Standardize Serialization.
     */

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
     * Creates Metric consumer that will attempt to find all the metric data within a directory
     * and push them to a queue.
     */
    public MetricConsumer(File directory, MetricQueue queue) {
        if (directory == null || !directory.isDirectory())
            throw new IllegalArgumentException("Illegal Data directory " + directory);
        if (queue == null)
            throw new NullPointerException("Metric Queue cannot be null");
        this.directory = directory;
        this.queue = queue;
    }

    /**
     * Attempts to process all the sub directories on the root directories for all possible
     * metrics.
     */
    @Override
    public void run() {
        log.info("Attempting to run {} at the root directory: {}",
                this.getClass().getSimpleName(), this.directory.getAbsolutePath());

        // Treat each individual directory with the root directory as its own isolated run.
        // The failure of processing one directory should not interfere with processing another.

        final Timer.Context runTimer = BalboaAgentMetrics.totalRuntime().time();
        Set<File> directories = FileUtils.getDirectories(directory);
        long start = System.currentTimeMillis();
        int recordsProcessed = 0;
        for (File dir: directories) {
            try {
                recordsProcessed += processDirectory(dir);
            } catch (IOException e) {
                log.error("Exception caught processing " + dir.getAbsolutePath(), e);
            }
        }
        long processingTime = System.currentTimeMillis() - start;
        log.info("Run completed, processed {} in {} ms", recordsProcessed, processingTime);
        BalboaAgentMetrics.metricsEmittedCount().inc(recordsProcessed);
        BalboaAgentMetrics.metricsEmittedMeter().mark(recordsProcessed);
        runTimer.stop();
    }

    /**
     * Closing the Metric Consumer is effectively a delegation method that allows the Metric Consumers
     * internal resources to close and clean up (If necessary).
     *
     * @throws IOException When there is a problem closing the queue.
     */
    @Override
    public void close() throws Exception {
        try {
            // Close and allow any clean up functionality to occur. IE. Flushing out a buffer.
            queue.close();
        } catch (Exception e) {
            log.error("Error shutting down Metric Consumer", e);
            throw e;
        }
    }

    /**
     * Given a directory, process all the metric files at the root of this directory.
     *
     * @param dir The root directory of all metric files.
     * @return t The number of {@link MetricsRecord}s processed.
     * @throws IOException if there are any issues processing the directory.
     */
    private int processDirectory(File dir) throws IOException {
        assert dir.isFile(): String.format("%s must be a directory", dir.getAbsolutePath());

        long start = System.currentTimeMillis();
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

                List<MetricsRecord> records;

                try {
                    records = processFile(metricsEventLog);
                } catch (IOException e) {
                    log.error("Error reading records from {}", metricsEventLog, e);
                    BalboaAgentMetrics.metricsProcessingFailureCounter().inc();
                    File broken = new File(metricsEventLog.getAbsolutePath() + FileUtils.BROKEN_FILE_EXTENSION);
                    if (!metricsEventLog.renameTo(broken)) {
                        log.warn("Unable to rename broken file {} permissions issue?", metricsEventLog);
                        BalboaAgentMetrics.renameBrokenFileFailureCounter().inc();
                    }
                    continue;
                }

                for (MetricsRecord r : records) {
                    queue.create(
                            new Fluff(r.getEntityId()),
                            new Fluff(r.getName()),
                            r.getValue().longValue(),
                            r.getTimestamp(),
                            r.getType()
                    );
                }

                recordsProcessed += records.size();
                if(!metricsEventLog.delete()) {
                    log.error("Unable to delete event log {} - file may be read twice, which is bad.", metricsEventLog);
                    BalboaAgentMetrics.deleteEventFailureCounter().inc();
                }
            }
        }
        long processingTime = System.currentTimeMillis() - start;
        BalboaAgentMetrics.singleDirectoryRuntimeHistogram().update(processingTime);
        BalboaAgentMetrics.singleDirectoryNumProcessedHistogram().update(recordsProcessed);
        return recordsProcessed;
    }

    private static final Pattern integerPattern = Pattern.compile("-?[0-9]+");

    /**
     * Given a metrics data file, attempt to extract all the metrics from the file and
     * pushes these metrics into the underlying queue.
     *
     * @param f File to process.
     * @return A list of {@link MetricsRecord}s that were process.
     * @throws IOException When there is a problem processing the file.
     */
    private List<MetricsRecord> processFile(File f) throws IOException {
        String filePath = f.getAbsolutePath();
        log.info("Processing file {}", filePath);
        List<MetricsRecord> results = new ArrayList<>();
        InputStream stream = new BufferedInputStream(new FileInputStream(f));
        try {
            Map<String, String> record;
            while ((record = readRecord(stream)) != null) {
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

                results.add(new MetricsRecord(record.get(ENTITY_ID), record.get(NAME),
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

    /**
     * Grovel accepts a stream of bytes and deserialize them into a single Metric.
     *
     * <b>
     *  This class works under the assumptions that bytes are serialized under the following format
     *  <ul>
     *      <li>0xff - single byte - Beginning mark of a single metrics entry</li>
     *      <li>Sequence of bytes of undetermined length - A utf-8 byte String encoded to bytes of a timestamp of type long.</li>
     *      <li>0xfe - single byte - end of timestamp sequence</li>
     *      <li>Sequence of bytes of undetermined length - A utf-8 byte String encoded to bytes of the entity id</li>
     *      <li>0xfe - single byte - end of entity id byte sequence</li>
     *      <li>Sequence of bytes of undetermined length - A utf-8 byte String encoded to bytes of the metric name</li>
     *      <li>0xfe - single byte - end of metric name byte sequence</li>
     *      <li>Sequence of bytes of undetermined length - A utf-8 byte String encoded to bytes of the metric value.  The metric value is of type Number</li>
     *      <li>0xfe - single byte - end of metric value byte sequence</li>
     *      <li>Sequence of bytes of undetermined length - A utf-8 byte String encoded to bytes of the metric type.  See {@link com.socrata.balboa.metrics.Metric.RecordType}</li>
     *      <li>0xfe - single byte - end of metric type byte sequence</li>
     *  </ul>
     * </b>
     *
     * @param stream A stream of bytes that represent a Single Metric.
     * @return A Map representing a single Metric entry. null if end of file has been reached.
     * @throws IOException If an incomplete metrics record was identified.
     */
    private Map<String, String> readRecord(InputStream stream) throws IOException {
        // First we have to find the start-of-record (a 0xff byte).
        // It *should* be the very first byte we're looking at.
        if (!seekToHeadOfMetrics(stream))
            return null;

        Map<String, String> record = new HashMap<>();
        for (String field : fields) {
            String fieldValue = readField(stream);
            if (fieldValue == null) { // This was the last record was processed with the prior iteration.
                return null;
            }
            record.put(field, fieldValue);
        }
        return record;
    }

    /**
     * Given a stream of bytes that does not begin with 0xff, read the bytes until 0xfe is reached.  The
     * resulting bytes read will be interpreted as a utf-8 String representation of the field value.
     *
     * @param stream Stream of bytes that represent a field value.
     * @return utf-8 String representation of the bytes read.
     * @throws IOException An error occurred while processing the Stream.
     */
    private String readField(InputStream stream) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int b;
        while ((b = stream.read()) != 0xfe) {
            switch(b) {
                case -1: // EOF
                    int baosSize = baos.size();
                    if (baosSize > 0)
                        log.warn("Reached end of file with {} bytes not processed.  This could mean lost or corrupted " +
                                "metrics data", baosSize);
                    return null;
                case 0xff:
                    log.warn("Found an incomplete record.");
                    BalboaAgentMetrics.incompleteFieldCounter().inc();
                    throw new IOException("Unexpected 0xFF field in file. Refusing to continue " +
                            "to process since our file is almost certainly corrupt.");
                default: // Expect that all other bytes are apart of the field.
                    baos.write(b);
            }
        }
        // We have successfully found the end of the metric field.
        return new String(baos.toByteArray(), "utf-8");
    }

    /**
     * Given a stream of bytes that represent a stream of serialized metrics entries, this method
     * reads bytes off of the stream until it reads the 0xff or EOF.  Once the head of the metrics
     * sequence is read the stream will be set to read the next byte.
     *
     * <br>
     * NOTE: This method does alter the state of the input stream.
     * </br>
     *
     * @param stream The InputStream of bytes.
     * @return True whether the Stream is ready to read. False if the Stream does not provide any metrics.
     * @throws IOException There is a problem reading from the stream.
     */
    private boolean seekToHeadOfMetrics(InputStream stream) throws IOException {
        int b;
        while ((b = stream.read()) != -1) {
            switch (b) {
                case 0xff:
                    return true;
                default:
                    log.warn("Unexpected byte: {} found.  Continuing to seek until " +
                            "head of metrics.", b);
            }
        }
        // Reached EOF
        return false;
    }

    @Override
    public String toString() {
        return "MetricConsumer{" +
                "directory=" + directory +
                ", queue=" + queue +
                '}';
    }
}
