package com.socrata.balboa.metrics;

import com.socrata.balboa.metrics.data.CompoundIterator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.IOException;
import java.util.*;

public class Metrics
{
    Map<String, Metric> metrics;
    Long timestamp;

    public Metrics()
    {
        metrics = new HashMap<String, Metric>();
    }

    public Metrics(int size)
    {
        metrics = new HashMap<String, Metric>(size);
    }

    public void merge(Metrics other)
    {
        // Get the union of the two key sets.
        Set<String> unionKeys = new HashSet<String>(getMetrics().keySet());
        unionKeys.addAll(other.getMetrics().keySet());

        // Combine the two maps.
        for (String key : unionKeys)
        {
            if (getMetrics().containsKey(key))
            {
                getMetrics().get(key).combine(other.getMetrics().get(key));
            }
            else if (other.getMetrics().containsKey(key))
            {
                getMetrics().put(key, other.getMetrics().get(key));
            }
        }
    }

    public static Metrics summarize(Iterator<Metrics>... everything) throws IOException
    {
        Metrics metrics = new Metrics();

        Iterator<Metrics> iter = new CompoundIterator<Metrics>(everything);

        while (iter.hasNext())
        {
            metrics.merge(iter.next());
        }

        return metrics;
    }

    @JsonProperty("__timestamp__")
    public Long getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(Long timestamp)
    {
        this.timestamp = timestamp;
    }

    public Map<String, Metric> getMetrics()
    {
        return metrics;
    }

    public void put(String name, Metric value)
    {
        getMetrics().put(name, value);
    }

    public Metric get(String name)
    {
        return getMetrics().get(name);
    }

    public Set<String> keySet()
    {
        return getMetrics().keySet();
    }

    public boolean containsKey(String name)
    {
        return getMetrics().containsKey(name);
    }

    public int size()
    {
        return getMetrics().size();
    }
}
