package com.socrata.balboa.metrics;

import com.socrata.balboa.metrics.measurements.combining.Absolution;
import com.socrata.balboa.metrics.measurements.combining.Combinator;
import com.socrata.balboa.metrics.measurements.combining.Summation;

public class Metric
{
    public enum RecordType {
        AGGREGATE,
        ABSOLUTE;

        @Override
        public String toString()
        {
            return super.toString().toLowerCase();
        }

        public String tag()
        {
            return "[" + toString() + "]";
        }
    }

    Number value;
    RecordType type;

    public Metric(RecordType type, Number value)
    {
        this.type = type;
        this.value = value;
    }

    public Number getValue()
    {
        return value;
    }

    public void setValue(Number value)
    {
        this.value = value;
    }

    public RecordType getType()
    {
        return type;
    }

    public void setType(RecordType type)
    {
        this.type = type;
    }

    public void combine(Metric other)
    {
        if (other == null)
        {
            return;
        }
        
        if (getType() != other.getType())
        {
            throw new IllegalArgumentException("Cannot combine two differently typed metrics (" + type + ", " + other.getType() + ")");
        }

        Combinator<Number> com;

        switch (getType())
        {
            case AGGREGATE: com = new Summation(); break;
            case ABSOLUTE: com = new Absolution(); break;
            default:
                throw new IllegalArgumentException("Unsupported combination type '" + getType() + "'.");
        }

        setValue(com.combine(getValue(), other.getValue()));
    }
}
