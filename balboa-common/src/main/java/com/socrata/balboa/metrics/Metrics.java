package com.socrata.balboa.metrics;

import java.io.IOException;
import java.util.*;
import java.util.Map;
import java.util.Set;

/**
 * A bag of metrics which can be merged through summation or
 * replace operations.
 * <p/>
 * Warning; merge operations on Metrics are not immutable.
 */
public class Metrics extends HashMap<String, Metric> {
    public Metrics(int i, float v) {
        super(i, v);
    }

    public Metrics(int i) {
        super(i);
    }

    public Metrics() {
        super();
    }

    public Metrics(Map<? extends String, ? extends Metric> map) {
        super(map);
    }

    public Set<Map.Entry<String, Metric>> difference(Metrics other) {
        Set<Map.Entry<String, Metric>> union = new HashSet<>(entrySet());
        union.addAll(other.entrySet());

        Set<Map.Entry<String, Metric>> intersection = new HashSet<>(entrySet());
        intersection.retainAll(other.entrySet());

        union.removeAll(intersection);

        return union;
    }

    public Metrics filter(String pattern) {
        Metrics results = new Metrics(size());

        for (Map.Entry<String, Metric> entry : entrySet()) {
            if (entry.getKey().matches(pattern)) {
                results.put(entry.getKey(), entry.getValue());
            }
        }

        return results;
    }

    public Metrics combine(String pattern) {
        Metrics results = new Metrics(1);
        Metric combined = null;

        for (Map.Entry<String, Metric> entry : entrySet()) {
            if (entry.getKey().matches(pattern)) {
                if (combined == null) {
                    combined = entry.getValue();
                } else {
                    combined.combine(entry.getValue());
                }
            }
        }

        if (combined == null) {
            combined = new Metric(Metric.RecordType.AGGREGATE, 0);
        }

        results.put("result", combined);

        return results;
    }

    /**
     * Merges another collection of metrics with this.  Metrics of the same name that are found in both this and
     * the other Metrics object are then combined via the {@link Metric#combine(Metric)} method.
     *
     * @param other Other Metrics to combine with this.
     */
    public Metrics merge(Metrics other) {
        // Get the union of the two key sets.
        Set<String> unionKeys = new HashSet<>(keySet());
        unionKeys.addAll(other.keySet());

        // Combine the two maps.
        for (String key : unionKeys) {
            if (containsKey(key)) {
                get(key).combine(other.get(key));
            } else if (other.containsKey(key)) {
                put(key, other.get(key));
            }
        }

        return this;
    }

    public static Metrics summarize(scala.collection.Iterator<Metrics> metricsIterator) throws IOException {
        Metrics metrics = new Metrics();

        while (metricsIterator.hasNext()) {
            metrics.merge(metricsIterator.next());
        }

        return metrics;
    }
}
