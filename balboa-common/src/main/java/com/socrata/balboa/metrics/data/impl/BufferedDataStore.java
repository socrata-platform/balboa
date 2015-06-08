package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.Metrics;
import com.socrata.balboa.metrics.Timeslice;
import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.config.ConfigurationException;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Buffers metrics across all metric sources for some
 * time period. This only keeps a single buffer for the
 * current time slice.
 *
 * Metrics with timestamps older than the current slice:
 *    If a metric comes in with a timestamp older than the
 *    current slice it will be passed to the underlying
 *    datastore immediately.
 *
 * Metrics in the current slice:
 *    If a metric comes in with a timestamp within the current
 *    slice it will be aggregated
 *
 * Metrics in the future:
 *    Metrics in the future will trigger a flush of the
 *    buffer and the current slice will be set to the
 *    future timestamp of that metric.
 */
public class BufferedDataStore extends DataStoreImpl {
    private static final Logger log = LoggerFactory.getLogger(BufferedDataStore.class);
    public  final long AGGREGATE_GRANULARITY;
    private final DataStore underlying;
    private long currentSlice = -1;
    private final Map<String, Metrics> buffer = new HashMap<String, Metrics>();
    private final TimeService timeService;

    public BufferedDataStore(DataStore underlying) {
        this(underlying, new TimeService());
    }

    public BufferedDataStore(DataStore underlying, TimeService timeService) {
        this.underlying = underlying;
        this.timeService = timeService;
        try {
            Configuration config = Configuration.get();
            AGGREGATE_GRANULARITY = Long.parseLong(config.getProperty("buffer.granularity"));
        } catch (IOException e) {
            throw new ConfigurationException("BufferedDataStore Configuration Error", e);
        }

    }

    public void heartbeat() {
        long timestamp = timeService.currentTimeMillis();
        long nearestSlice = timestamp - (timestamp % AGGREGATE_GRANULARITY);
        if (nearestSlice <= currentSlice)
            return;
        try {
            flushExpired(timestamp);
        } catch(IOException e) {
            log.error("Unable to flush buffered metrics at regular heartbeat. This is bad.");

        }
    }

    public void flushExpired(long timestamp) throws IOException {
        synchronized (buffer) {
            long nearestSlice = timestamp - (timestamp % AGGREGATE_GRANULARITY);
            if (nearestSlice > currentSlice) {
                log.info("Flushing " + buffer.size() + " entities to underlying datastore from the last " + AGGREGATE_GRANULARITY + "ms");
                // flush metrics
                for (String entity : buffer.keySet()) {
                    // If a failure occurs in the underlying datastore the exception
                    // chain back up and keep the buffer in memory
                    log.info("  flushing " + entity);
                    underlying.persist(entity, currentSlice, buffer.get(entity));
                }
                buffer.clear();
                currentSlice = nearestSlice;
            }
        }
    }

    public void persist(String entityId, long timestamp, Metrics metrics) throws IOException {
        synchronized (buffer) {
            if (timestamp < currentSlice) {
                // Metrics older than our current slice do not get aggregated.
                underlying.persist(entityId, timestamp, metrics);
                return;
            }
            flushExpired(timestamp);
            Metrics existing = buffer.get(entityId);
            if (existing != null) {
                existing.merge(metrics);
            } else {
                buffer.put(entityId, metrics);
            }
        }

    }

    public Iterator<String> entities(String pattern) throws IOException {
        return underlying.entities(pattern);
    }

    public Iterator<String> entities() throws IOException {
        return underlying.entities();
    }

    public Iterator<Timeslice> slices(String entityId, Period period, Date start, Date end) throws IOException {
        return underlying.slices(entityId, period, start, end);
    }

    public Iterator<Metrics> find(String entityId, Period period, Date date) throws IOException {
        return underlying.find(entityId, period, date);
    }

    public Iterator<Metrics> find(String entityId, Period period, Date start, Date end) throws IOException {
        return underlying.find(entityId, period, start, end);
    }

    public Iterator<Metrics> find(String entityId, Date start, Date end) throws IOException {
        return underlying.find(entityId, start, end);
    }

    @Override
    public void onStop() {
        heartbeat();
    }
}
