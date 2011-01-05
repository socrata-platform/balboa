package com.socrata.balboa.metrics;

import com.socrata.balboa.metrics.data.CompoundIterator;

import java.io.IOException;
import java.util.*;

public class Metrics extends HashMap<String, Metric>
{
    public Metrics(int i, float v)
    {
        super(i, v);
    }

    public Metrics(int i)
    {
        super(i);
    }

    public Metrics()
    {
        super();
    }

    public Metrics(Map<? extends String, ? extends Metric> map)
    {
        super(map);
    }

    public Metrics filter(String pattern)
    {
        Metrics results = new Metrics(size());

        for (Map.Entry<String, Metric> entry : entrySet())
        {
            if (entry.getKey().matches(pattern))
            {
                results.put(entry.getKey(), entry.getValue());
            }
        }

        return results;
    }

    public Metrics combine(String pattern)
    {
        Metrics results = new Metrics(1);
        Metric combined = null;

        for (Map.Entry<String, Metric> entry : entrySet())
        {
            if (entry.getKey().matches(pattern))
            {
                if (combined == null)
                {
                    combined = entry.getValue();
                }
                else
                {
                    combined.combine(entry.getValue());
                }
            }
        }

        results.put("result", combined);

        return results;
    }

    public void merge(Metrics other)
    {
        // Get the union of the two key sets.
        Set<String> unionKeys = new HashSet<String>(keySet());
        unionKeys.addAll(other.keySet());

        // Combine the two maps.
        for (String key : unionKeys)
        {
            if (containsKey(key))
            {
                get(key).combine(other.get(key));
            }
            else if (other.containsKey(key))
            {
                put(key, other.get(key));
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
}
