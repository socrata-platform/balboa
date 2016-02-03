package com.blist.metrics.impl.queue;

import com.socrata.balboa.metrics.Metric;
import com.socrata.metrics.IdParts;
import com.socrata.metrics.MetricQueue$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * A Metric Queue that writes metrics to data files on disk in a specified directory.
 *
 * Provides a Singleton and Non-Singleton implementation.  The Singleton implementation is exposed through the
 * {@link MetricFileQueue#getInstance(File)} function.  The Non Singleton implementation is exposed through the
 * constructor.
 */
public class MetricFileQueue extends AbstractJavaMetricQueue {
    private static final Logger log = LoggerFactory.getLogger(MetricFileQueue.class);

    /*
    TODO clearly evident requirement for restricted write access to data logs but adhoc over use of singleton instances.
    TODO Singleton are impractical in this situation.
     */

    /**
     * Mapping between directories and Metric File Queues.
     */
    private static Map<File, MetricFileQueue> instances = new HashMap<>();
    private static long MAX_METRICS_PER_FILE = 20000;

    private final File directory;
    private long reopenTime;
    private long metricCount = 0;

    private FileOutputStream fileStream = null;
    private BufferedOutputStream stream = null;

    /**
     * Creates a MetricFileQueue instance for a specific directory.
     *
     * @param directory The directory in which to write metrics data.
     */
    public MetricFileQueue(File directory) {
        if (!isDirectory(directory)) {
            throw new IllegalArgumentException("Illegal directory \"" + directory + "\". Cannot create Metrics File Queue.");
        }
        this.directory = directory;
    }

    /**
     * Provides the interface to the singleton instance for a specific directory.
     *
     * @param directory The directory in which to write metrics data.
     * @return The MetricFileQueue for a given directory.
     */
    public static synchronized MetricFileQueue getInstance(File directory) {
        MetricFileQueue q = instances.get(directory);
        if (q == null) {
            q = new MetricFileQueue(directory);
            instances.put(directory, q);
        }
        return q;
    }

    private void open() throws IOException {
        long now = System.currentTimeMillis();
        String basename = String.format("metrics2012.%016x", now);

        this.directory.mkdirs();

        String logName = basename + ".data";
        fileStream = new FileOutputStream(new File(directory, logName), true);
        stream = new BufferedOutputStream(fileStream);
        reopenTime = now + MetricQueue$.MODULE$.AGGREGATE_GRANULARITY();
        metricCount = 0;
    }

    @Override
    public synchronized void close() throws IOException {
        if (stream != null) {
            stream.close();
            fileStream = null;
            stream = null;
        }
    }

    public synchronized void create(String entityId, String name, Number value, long timestamp, Metric.RecordType type) {
        // File format:
        // 0xff asciiTimestamp 0xfe entityId 0xfe name 0xfe asciiNumber 0xfe asciType 0xfe

        try {
            if (stream == null) {
                open();
            } else if (System.currentTimeMillis() >= reopenTime || metricCount >= MAX_METRICS_PER_FILE) {
                close();
                open();
            }
            metricCount++;
            stream.write(0xff);

            stream.write(utf8(String.valueOf(timestamp)));
            stream.write(0xfe);

            stream.write(utf8(entityId));
            stream.write(0xfe);

            stream.write(utf8(name));
            stream.write(0xfe);

            stream.write(utf8(String.valueOf(value)));
            stream.write(0xfe);

            stream.write(utf8(String.valueOf(type)));
            stream.write(0xfe);

            stream.flush();

            // fileStream.getChannel().force(true);
            // fileStream.getFD().sync();
        } catch (IOException e) {
            log.error("Exception writing data collection file", e);
            try {
                close();
            } catch (IOException e2) {
                log.error(
                        "Problem closing data collection file while handling exception writing data collection file",
                        e2);
            }
        }
    }

    public void create(IdParts entity, IdParts name, long value, long timestamp, Metric.RecordType type) {
        create(entity.toString(), name.toString(), value, timestamp, type);
    }

    public File getDirectory() {
        return directory;
    }

    private static byte[] utf8(String s) throws UnsupportedEncodingException {
        return s.getBytes("utf-8");
    }

    private static boolean isDirectory(File directory) {
        return directory != null && directory.isDirectory();
    }
}
