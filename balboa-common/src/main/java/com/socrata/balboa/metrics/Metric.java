package com.socrata.balboa.metrics;

import com.socrata.balboa.metrics.measurements.combining.Absolution;
import com.socrata.balboa.metrics.measurements.combining.Combinator;
import com.socrata.balboa.metrics.measurements.combining.Summation;
import org.codehaus.jackson.annotate.JsonCreator;

/**
 * Represents a single a Metric.
 *
 * <p>
 *     Currently individual Metrics are identified by a type and numeric value.
 *     <table>
 *         <tr><th>Record Type</th><th>Definition</th></tr>
 *         <tr><td>Aggregate</td><td>A Metric that is to be aggregated with other metrics of the same name.</td></tr>
 *         <tr><td>Absolute</td><td>A Metric that is never to be changed.  Metrics of this type are selected by the
 *         latest occurrence of the metric of this name.</td></tr>
 *     </table>
 * </p>
 */
public class Metric {

    /*
        TODO Make Metric Class Immutable
        TODO Metric name should be included
        TODO Port to class to Scala
        TODO Make Metric sealed trait
        TODO Make Absolute Metric Sub Type
        TODO Make Aggreagate Metric Sub Type.
        TODO Handle typing and subclassing
        TODO Should be immutable.
     */

    /**
     * There is the ability to pre aggregate metrics with additional clients or services.  This enumeration
     * allows you to defined whether your metric is pre aggregated or in absolute form.  You can think of this
     * enumeration as a characteristic tag for a specific metric.
     *
     * <br>
     *     Example Absolute Metric: Number of the datasets that exists at any given time.
     * <br>
     *     Example Aggregate Metric: Total data sets downloaded
     */
    public enum RecordType {
        AGGREGATE,
        ABSOLUTE;

        @Override
        public String toString() {
            return super.toString().toLowerCase();
        }

        @JsonCreator
        public static RecordType fromString(String value) {
            return RecordType.valueOf(value.toUpperCase());
        }
    }

    Number value;
    RecordType type;

    public Metric() {
    }

    @Override
    public int hashCode() {
        return type.hashCode() + value.hashCode();
    }

    // TODO Is there a reason we are being a little looser with equality using instanceof instead of class equality?
    @Override
    public boolean equals(Object o) {
        if (o instanceof Metric) {
            Metric other = (Metric) o;
            return other.type == type && other.value.equals(value);
        } else {
            return false;
        }
    }

    public Metric(RecordType type, Number value) {
        this.type = type;
        this.value = value;
    }

    public Number getValue() {
        return value;
    }

    public void setValue(Number value) {
        this.value = value;
    }

    public RecordType getType() {
        return type;
    }

    public void setType(RecordType type) {
        this.type = type;
    }

    /**
     * Combines this metric with another of the same type.  Uses the default Combinator for this Metric type.
     *
     * @param other Other metric to Combine with.
     * @throws java.lang.IllegalArgumentException If the other metric is of the wrong type.
     */
    public void combine(Metric other) {
        combine(other, null);
    }

    /**
     * Combines this metric with another using a Combinator.  If the combinator is null then it is inferred based off
     * of the Metric type.  If other is null then no action is taken.
     *
     * Preconditions:
     *      - other Metric must have the same type.
     *
     * @param other Other metric to combine with
     * @param com Combinator.  If null defaults to combination action based on the Metric Type.
     */
    public void combine(Metric other, Combinator<Number> com) {
        // TODO There is no reason these metrics should not be immutable.
        if (other == null) {
            return;
        }

        if (getType() != other.getType()) {
            throw new IllegalArgumentException("Cannot combine two differently typed metrics (" + type + ", " + other.getType() + ")");
        }

        if (com == null) { // Defaults to type specific combiner.
            switch (getType()) {
                case AGGREGATE:
                    com = new Summation();
                    break;
                case ABSOLUTE:
                    com = new Absolution();
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported combination type '" + getType() + "'.");
            }
        }

        setValue(com.combine(getValue(), other.getValue()));
    }

    @Override
    public String toString() {
        return "Metric{" +
                "value=" + value +
                ", type=" + type +
                '}';
    }
}
