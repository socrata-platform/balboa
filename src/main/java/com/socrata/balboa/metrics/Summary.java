package com.socrata.balboa.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

public class Summary
{
    private static Log log = LogFactory.getLog(Summary.class);

    public static enum Type
    {
        YEARLY,
        MONTHLY,
        WEEKLY,
        DAILY,
        REALTIME;

        @Override
        public String toString()
        {
            return this.name().toLowerCase();
        }

        public Type nextBest()
        {
            switch(this)
            {
                case YEARLY:
                    return MONTHLY;
                case MONTHLY:
                case WEEKLY:
                    // Because weeks don't fall on month boundaries we can't use them as a "next best" for months. At
                    // most we're talking about reading 31 values for the daily summaries anyway which isn't really that
                    // bad.
                    return DAILY;
                case DAILY:
                    return REALTIME;
                default:
                    return null;
            }
        }
    };

    public Summary(Type type, long timestamp, Map<String, String> values)
    {
        this(timestamp, values);
        this.type = type;
    }

    public Summary(long timestamp, Map<String, String> values)
    {
        this.timestamp = timestamp;
        this.values = values;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public Type getType()
    {
        return type;
    }

    public Map<String, String> getValues()
    {
        return values;
    }

    long timestamp;
    Map<String, String> values;
    Type type;
}
