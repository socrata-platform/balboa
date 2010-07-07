package com.socrata.balboa.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

/**
 * A summary is a collection of metrics data covering some time range. The
 * metric data is in the values hash and the date range that the summary covers
 * (or will cover) is the type.
 *
 * Multiple summaries can be combined so they cover a date range outside of
 * their type.
 *
 * The timestamp on summaries should generally be the first millisecond in the
 * time area that it covers, but can technically be any time within the range
 * itself.
 */
public class Summary
{
    private static Log log = LogFactory.getLog(Summary.class);

    /**
     * The type of summary. Types fall on boundaries that are convenient and
     * easily queried.
     */
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

        /**
         * Retrieve the adjacent type that is more granular than the current.
         * For example, "day" is slightly more granular than "month" which is
         * slightly more granular than "year".
         */
        public Type nextBest()
        {
            switch(this)
            {
                case YEARLY:
                    return MONTHLY;
                case MONTHLY:
                case WEEKLY:
                    // Because weeks don't fall on month boundaries we can't
                    // summarize them as the next best of months.
                    return DAILY;
                case DAILY:
                    return REALTIME;
                default:
                    return null;
            }
        }
    };

    public Summary(Type type, long timestamp, Map<String, Object> values)
    {
        this.type = type;
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

    public Map<String, Object> getValues()
    {
        return values;
    }

    long timestamp;
    Map<String, Object> values;
    Type type;
}
