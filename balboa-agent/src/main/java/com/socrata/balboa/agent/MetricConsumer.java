package com.socrata.balboa.agent;

import com.codahale.metrics.Timer;
import com.socrata.balboa.agent.metrics.BalboaAgentMetrics;
import com.socrata.balboa.metrics.Metric;
import com.socrata.balboa.util.FileUtils;
import com.socrata.metrics.Fluff;
import com.socrata.metrics.MetricQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * The MetricConsumer consumes metrics from data files from within a specific directory.  Any metrics extracted will
 * be pushed to a specified {@link MetricQueue}.
 *
 * <br>
 *
 * Given a root directory, the MetricConsumer will recursively extract all the directories under the root directory.
 * For each directory, files are processed in reverse alphabetical order excluding the last file in the order.  It is
 * common and suggested that metric producers use timestamps in their file names and not place any additional files
 * within the same directory they write metrics to.
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
    private final MetricQueue metricPublisher;
    private final MetricFileProvider fileProvider;

    /**
     * Creates Metric consumer that will attempt to find all the metric data within a directory
     * and push them to a metricPublisher. Does not take responsibility for closing the metricPublisher.
     */
    public MetricConsumer(File directory, MetricQueue metricPublisher) {
        this(directory, metricPublisher, new AlphabeticMetricFileProvider(directory.toPath()));
    }

    /**
     * Creates a MetricConsumer that processes files from `directory` and emits them to `metricPublisher`.  The `fileProvider`
     * is a control mechanism that allows clients to make determinations on which {@link File}s can be processed.
     *
     * @param directory Directory in which to process metrics.
     * @param metricPublisher Queue to emit metrics to.
     * @param fileProvider The {@link FileFilter} used to determine which files are allowed to be processed.
     */
    public MetricConsumer(File directory, MetricQueue metricPublisher, MetricFileProvider fileProvider) {
        if (directory == null || !directory.isDirectory())
            throw new IllegalArgumentException("Illegal Data directory " + directory);
        if (metricPublisher == null)
            throw new NullPointerException("Metric Queue cannot be null");
        this.directory = directory;
        this.metricPublisher = metricPublisher;
        this.fileProvider = fileProvider;
    }

    /**
     * Attempts to process all the sub directories on the root directories for all possible
     * metrics.
     */
    @Override
    public void run() {
        log.info("Looking for metrics files recursively in '{}'", this.directory.getAbsolutePath());

        final Timer.Context runTimer = BalboaAgentMetrics.totalRuntime().time();
        long start = System.currentTimeMillis();
        int recordsProcessed = 0;


        // Treat each individual Metric data file as its own isolated run.
        // We are trying to prevent the failure to process one file from blocking or preventing the processing
        // of others.
        for (File metricsEventLog: fileProvider.provideForJava()) {
            log.info("Processing '{}'.", metricsEventLog.getAbsolutePath());

            List<MetricsRecord> records;
            try {
                records = processFile(metricsEventLog);
            } catch (IOException e) {
                log.error("Error reading records from {}", metricsEventLog, e);
                BalboaAgentMetrics.metricsProcessingFailureCounter().inc();
                File broken = new File(metricsEventLog.getAbsolutePath() + FileUtils.BROKEN_FILE_EXTENSION());
                if (!metricsEventLog.renameTo(broken)) {
                    log.warn("Unable to rename broken file {} permissions issue?", metricsEventLog);
                    BalboaAgentMetrics.renameBrokenFileFailureCounter().inc();
                }
                continue;
            }

            for (MetricsRecord r : records) {
                metricPublisher.create(
                        new Fluff(r.entityId()),
                        new Fluff(r.name()),
                        r.value().longValue(),
                        r.timestamp(),
                        r.metricType()
                );
            }

            recordsProcessed += records.size();
            if(!metricsEventLog.delete()) {
                log.error("Unable to delete event log {} - file may be read twice, which is bad.", metricsEventLog);
                BalboaAgentMetrics.deleteEventFailureCounter().inc();
            }
        }

        long processingTime = System.currentTimeMillis() - start;
        log.info("Run completed, processed {} in {} ms", recordsProcessed, processingTime);
        BalboaAgentMetrics.metricsEmittedCount().inc(recordsProcessed);
        BalboaAgentMetrics.metricsEmittedMeter().mark(recordsProcessed);
        runTimer.stop();
    }

    /**
     * This method does nothing. As the MetricConsumer does not take unique
     * ownership of its metricPublisher, it cannot be sure that it is safe to close.
     */
    public void close() throws Exception {}

    private static final Pattern integerPattern = Pattern.compile("-?[0-9]+");

    /**
     * Given a metrics data file, attempt to extract all the metrics from the file and
     * pushes these metrics into the underlying metricPublisher.
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
     * Accepts a stream of bytes and deserialize them into a single Metric.
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
                default: // Expect that all other bytes are apart of the field value.
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
                ", metricPublisher=" + metricPublisher +
                '}';
    }
}
