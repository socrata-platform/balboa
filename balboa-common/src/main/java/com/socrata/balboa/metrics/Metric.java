package com.socrata.balboa.metrics;

import com.socrata.balboa.metrics.measurements.combining.Absolution;
import com.socrata.balboa.metrics.measurements.combining.Combinator;
import com.socrata.balboa.metrics.measurements.combining.Summation;
import org.codehaus.jackson.annotate.JsonCreator;

public class Metric {

    /**
     * There is the ability to pre aggregate metrics with additional clients or services.  This enumeration
     * allows you to defined whether your metric is pre aggregated or in absolute form.  You can think of this
     * enumeration as a characteristic tag for a specific metric.
     *
     * <br>
     *     Example Absolute Metric:
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
            return other.type.equals(type) && other.value.equals(value);
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
     * Given another Metric of the same type, combine and mutate this Metric with the respective {@link Combinator}.
     *
     * @param other The Metric to combine with.
     * @throws IllegalArgumentException in the case that the other Metric is not of the same {@link RecordType}
     */
    public void combine(Metric other) {
        if (other == null) {
            return;
        }

        if (getType() != other.getType()) {
            throw new IllegalArgumentException("Cannot combine two differently typed metrics (" + type + ", " + other.getType() + ")");
        }

        Combinator<Number> com;

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
