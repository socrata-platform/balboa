package com.socrata.balboa.metrics;

import com.socrata.balboa.metrics.measurements.serialization.JsonSerializer;
import com.socrata.balboa.metrics.measurements.serialization.Serializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;
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

    public Map<String, Object> getProcessedValues() throws IOException
    {
        // TODO: Memoize?
        // TODO: Replace getValues() with this? I think after this large
        //       refactor we won't need getValues() at all.
        Map<String, Object> results = new HashMap<String, Object>();
        Serializer serializer = new JsonSerializer();

        for (String key : getValues().keySet())
        {
            results.put(key, serializer.toValue(getValues().get(key)));
        }
        return results;
    }

    long timestamp;
    Map<String, String> values;
    Type type;
}
