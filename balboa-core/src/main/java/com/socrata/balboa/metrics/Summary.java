package com.socrata.balboa.metrics;

import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.data.Period;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

/**
 * A summary is a collection of metrics data covering some time range. The
 * metric data is in the values hash and the date range that the summary covers
 * (or will cover) is the period.
 *
 * Multiple summaries can be combined so they cover a date range outside of
 * their period.
 *
 * The timestamp on summaries should generally be the first millisecond in the
 * time area that it covers, but can technically be any time within the range
 * itself.
 */
public class Summary
{
    private static Log log = LogFactory.getLog(Summary.class);

    public Summary(Period period, long timestamp, Map<String, Object> values)
    {
        this.period = period;
        this.timestamp = timestamp;
        this.values = values;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public Period getType()
    {
        return period;
    }

    public Map<String, Object> getValues()
    {
        return values;
    }

    public void setValues(Map<String, Object> values)
    {
        this.values = values;
    }

    long timestamp;
    Map<String, Object> values;
    Period period;
}
