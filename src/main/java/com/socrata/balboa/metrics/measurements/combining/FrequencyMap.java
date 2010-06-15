package com.socrata.balboa.metrics.measurements.combining;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FrequencyMap implements Combinator<Map<String, Number>>
{
    @Override
    public Map<String, Number> combine(Map<String, Number> first, Map<String, Number> second)
    {
        Sum summer = new Sum();

        if (first == null)
        {
            first = new HashMap<String, Number>();
        }

        if (second == null)
        {
            second = new HashMap<String, Number>();
        }

        Set<String> mergedKeys = new HashSet<String>(first.keySet());
        mergedKeys.addAll(second.keySet());

        Map<String, Number> results = new HashMap<String, Number>(mergedKeys.size());
        for (String key : mergedKeys)
        {
            results.put(key, summer.combine(first.get(key), second.get(key)));
        }
        return results;
    }
}
