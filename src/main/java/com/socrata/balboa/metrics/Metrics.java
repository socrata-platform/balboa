package com.socrata.balboa.metrics;

import com.socrata.balboa.metrics.data.CompoundIterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class Metrics extends HashMap<String, Metric>
{
    public Metrics()
    {
        super();
    }

    public Metrics(int size)
    {
        super(size);
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
