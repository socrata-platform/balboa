package com.blist.metrics.impl.queue;

import com.socrata.balboa.metrics.Metrics;
import com.socrata.metrics.MetricQueue$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * An in-memory metrics buffer that is optimized to condense the metric message footprint.  This is
 * current done by condensing the memory footprint of the metrics themselves.  Depending on the
 * {@link com.socrata.balboa.metrics.Metric.RecordType}, the buffer manipulates the data to conserve space
 * in memory.
 *
 * Note: Be very cautious using the Class.  Try to avoid it if you can.  Mutating metrics data in memory in the
 * event of a killed process can result in lost, unrecoverable data.
 *
 * @see Metrics
 * @see com.socrata.balboa.metrics.Metric
 * @see com.socrata.balboa.metrics.Metric.RecordType
 *
 * Created by michaelhotan on 1/29/16.
 */
public class MetricsBuffer {
    private static final Logger log = LoggerFactory.getLogger(MetricsBuffer.class);

    /**
     * Class initially copied from Metric Consumer in Coreserver to MetricJmsQueueNotSingleton to here.
     *
     * This class was created in an effort to prevent the overloading of a Queueing system.  For legacy
     * reasons we continue to support it.   This class a couple of known risks that we look to mitigate in
     * the future.
     * * Class mutates metrics data in memory.
     * * Additional buffering layer that in memory and can result to non recoverable metric loss.
     *
     * Individually the risks seem small but combined together can be very problematic.
     */

    /**
     * The internal buffer conserves the memory footprint.
     * TODO: from a functional perspective the internal buffer should probably be a Set but one thing at a time.
     */
    private final Map<String, MetricsBag> internalBuffer = new HashMap<>();

    /**
     * Adds a collection of metrics to this.
     *
     * @param entityId The id to associate the collection of metrics
     * @param data Mapping of metric names to associated value
     * @param timestamp The timestamp these metrics occured.
     */
    synchronized void add(String entityId, Metrics data, long timestamp) {
        long nearestSlice = timestamp - (timestamp %  MetricQueue$.MODULE$.AGGREGATE_GRANULARITY());
        String bufferKey = entityId + ":" + nearestSlice;
        MetricsBag notBuffered = new MetricsBag(entityId, data, nearestSlice);

        log.debug("Attempting to add {} to the write internal buffer", notBuffered);
        if (internalBuffer.containsKey(bufferKey)) {
            log.debug("Buffer already contains \"{}\".  Merging with existing value set of metrics.", notBuffered);
            MetricsBag buffered = internalBuffer.get(bufferKey);
            buffered.getData().merge(notBuffered.getData());
        } else {
            log.debug("Adding new entry: \"{}\" to the write internalBuffer", notBuffered);
            internalBuffer.put(bufferKey, notBuffered);
        }
    }

    /**
     * Returns and deletes all existing metrics.
     *
     * @return All the current internal metrics.
     */
    synchronized Collection<MetricsBag> popAll() {
        log.debug("Removing all metrics from existing buffer");
        // TODO Array list could be a set.
        Collection<MetricsBag> currentValues = new ArrayList<>(internalBuffer.values());
        internalBuffer.clear();
        return currentValues;
    }

    /**
     * @return The current size of the buffer.
     */
    synchronized int size() {
        return internalBuffer.size();
    }

}
