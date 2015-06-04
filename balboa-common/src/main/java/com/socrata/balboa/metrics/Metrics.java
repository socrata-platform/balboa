package com.socrata.balboa.metrics;

import com.socrata.balboa.metrics.data.CompoundIterator;
import com.socrata.balboa.metrics.measurements.combining.Combinator;
import com.socrata.balboa.metrics.measurements.combining.Summation;

import java.io.IOException;
import java.util.*;

/**
 * A collection of metrics.  Metrics are identified by a name key and the correlated Metric value.
 * <p/>
 * Warning; merge operations on Metrics are not immutable.
 */
public class Metrics extends HashMap<String, Metric> {

    /*
    - TODO: Migrate Metrics to Scala
    - TODO: Make Metrics a simpler data structure. Mapping name to Metric requires a lot of looping which can get tediously
    - TODO Implement solid functional practices.
    - TODO: combining data leads to data loss.
     */

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
        Set<Map.Entry<String, Metric>> union = new HashSet<Map.Entry<String, Metric>>(entrySet());
        union.addAll(other.entrySet());

        Set<Map.Entry<String, Metric>> intersection = new HashSet<Map.Entry<String, Metric>>(entrySet());
        intersection.retainAll(other.entrySet());

        union.removeAll(intersection);

        return union;
    }

    /**
     * Filter out metrics that don't match an inputted Regex Pattern.
     *
     * @param pattern Regex Pattern to match againse
     * @return The resulting Metrics after filter
     */
    public Metrics filter(String pattern) {
        if (pattern == null)
            return this;

        Metrics results = new Metrics(size());
        for (Map.Entry<String, Metric> entry : entrySet()) {
            if (entry.getKey().matches(pattern)) {
                results.put(entry.getKey(), entry.getValue());
            }
        }
        return results;
    }

    /**
     * Combines a Metric name based off of a provided pattern.  For all metrics that match that name
     *
     * @param pattern The Regex pattern to match the metric name.
     * @return A combined Single mappping the of the result of the matching and combining.  "result" => Number
     */
    public Metrics combine(String pattern) {
        return combine(pattern, null);
    }

    /**
     * Combines this collection of metrics by matching all metric names with a argument pattern.
     *
     * @param pattern Metric name pattern to match to.
     * @param com Explicit Combinator.  Can be null.ential
     * @return
     */
    public Metrics combine(String pattern, Combinator<Number> com) {
        Metrics results = new Metrics(1);
        Metric combined = null;

        for (Map.Entry<String, Metric> entry : entrySet()) {
            if (entry.getKey().matches(pattern)) {
                if (combined == null) {
                    combined = entry.getValue();
                } else {
                    combined.combine(entry.getValue(), com);
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
     * Mutates: This
     * Merges this with a new set of Metric.
     *
     * @param other The Metrics to merge with.
     */
    public void merge(Metrics other) {
        merge(other, null);
    }

    /**
     * Mutates: This
     * Merges this with a new set of Metric.
     *
     * @param other The Metrics to merge with.
     * @param com Combinator used to combine Metrics together.
     */
    public void merge(Metrics other, Combinator<Number> com) {
        // Get the union of the two key sets.
        Set<String> unionKeys = new HashSet<>(keySet());
        unionKeys.addAll(other.keySet());

        // Combine the two maps.
        for (String key : unionKeys) {
            if (containsKey(key)) {
                get(key).combine(other.get(key), com);
            } else if (other.containsKey(key)) {
                put(key, other.get(key));
            }
        }
    }

    @SafeVarargs
    public static Metrics summarize(Iterator<Metrics>... everything) throws IOException {
        Metrics metrics = new Metrics();

        Iterator<Metrics> iter = new CompoundIterator<>(everything);

        while (iter.hasNext()) {
            metrics.merge(iter.next());
        }

        return metrics;
    }
}
