package com.blist.metrics.impl.queue;

import com.socrata.balboa.metrics.Metrics;
import com.socrata.metrics.MetricQueue$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * An in-memory metrics buffer that is optimized to condense the metric message footprint.  This is
 * currently done by condensing the memory footprint of the metrics themselves.  Depending on the
 * {@link com.socrata.balboa.metrics.Metric.RecordType}, the buffer manipulates the data to conserve space
 * in memory.
 *
 * Note: Be very cautious while using the Class.  Mutating metrics data in memory in the
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
     * This class is intended to present clients with a simpler interface then exposing the internal buffer
     * directly.  By limiting functionality of the buffer to add and popAll we reduce the chance of unnecessary
     * side effects while allowing us to explore different options for the underlying implementation.
     *
     * TODO: Stronger Unit Tests
     * TODO: From a abstractions perspective the internal buffer should probably be a Set.
     */

    /**
     * The internal buffer conserves the memory footprint.
     */
    private final Map<String, MetricsBucket> internalBuffer = new HashMap<>();

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
        MetricsBucket notBuffered = new MetricsBucket(entityId, data, nearestSlice);

        log.debug("Attempting to add {} to the write internal buffer", notBuffered);
        if (internalBuffer.containsKey(bufferKey)) {
            log.debug("Buffer already contains \"{}\".  Merging with existing value set of metrics.", notBuffered);
            MetricsBucket buffered = internalBuffer.get(bufferKey);
            //When we returned a copy of the data, this merge does nothing. The state is completely thrown away.
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
    synchronized Collection<MetricsBucket> popAll() {
        log.debug("Removing all metrics from existing buffer");
        // TODO Array list could be a set.
        Collection<MetricsBucket> currentValues = new ArrayList<>(internalBuffer.values());
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
