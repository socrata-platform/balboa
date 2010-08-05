package com.socrata.balboa.metrics;

import com.socrata.balboa.metrics.data.DateRange;
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

    public Summary(DateRange.Type type, long timestamp, Map<String, Object> values)
    {
        this.type = type;
        this.timestamp = timestamp;
        this.values = values;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public DateRange.Type getType()
    {
        return type;
    }

    public Map<String, Object> getValues()
    {
        return values;
    }

    public void setValue(Map<String, Object> values)
    {
        this.values = values;
    }

    long timestamp;
    Map<String, Object> values;
    DateRange.Type type;
}
