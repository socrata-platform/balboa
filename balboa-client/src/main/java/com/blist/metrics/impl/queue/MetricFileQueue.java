package com.blist.metrics.impl.queue;

import com.socrata.balboa.common.logging.JavaBalboaLogging;
import com.socrata.balboa.metrics.Metric;
import com.socrata.metrics.IdParts;
import com.socrata.metrics.MetricQueue$;
import org.slf4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class MetricFileQueue extends AbstractJavaMetricQueue {

    private static final Logger log = JavaBalboaLogging.instance(MetricFileQueue.class);

    /**
     * Mapping between directories and Metric File Queues
     */
    private static Map<File, MetricFileQueue> instances = new HashMap<>();
    private static long MAX_METRICS_PER_FILE = 20000;

    private File directory;
    private long reopenTime;
    private long metricCount = 0;

    private FileOutputStream fileStream = null;
    private BufferedOutputStream stream = null;

    private MetricFileQueue(File directory) {
        this(directory, "");
    }

    private MetricFileQueue(File directory, String namespace) {
        this.directory = subdir(directory, namespace);
    }

    private static File subdir(File directory, String namespace) {
        if (namespace == null) {
            namespace = "";
        }
        if (namespace.isEmpty()) return directory;
        else return new File(directory, namespace);
    }

    public File getDirectory() {
        return directory;
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

    public synchronized void close() throws IOException {
        if (stream != null) {
            stream.close();
            fileStream = null;
            stream = null;
        }
    }

    public synchronized void create(String entityId, String name, Number value, long timestamp, Metric.RecordType type) {
        // File format:
        // 0xff asciiTimestamp 0xfe entityId 0xfe name 0xfe asciiNumber 0xfe

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

    private static byte[] utf8(String s) throws UnsupportedEncodingException {
        return s.getBytes("utf-8");
    }

    public static synchronized MetricFileQueue getInstance(File directory) {
        MetricFileQueue q = instances.get(directory);
        if (q == null) {
            directory.mkdir();
            q = new MetricFileQueue(directory);
            instances.put(directory, q);
        }
        return q;
    }

    public void create(IdParts entity, IdParts name, long value, long timestamp, Metric.RecordType type) {
        create(entity.toString(), name.toString(), value, timestamp, type);
    }
}
