package com.socrata.balboa.metrics.utils;

import com.socrata.balboa.metrics.measurements.combining.sum;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MetricUtils
{
    /**
     * Merge two maps into one another and place the results in the first map.
     * The exclusion is just added to the first map while the intersection is
     * combined using the "sum" combinator.
     *
     * @param first The first map and the map into which the results are placed
     * @param second The second map to merge.
     *
     * @return The resulting merged map.
     */
    public static Map<String, Object> merge(Map<String, Object> first, Map<String, Object> second)
    {
        // Get the union of the two key sets.
        Set<String> unionKeys = new HashSet<String>(first.keySet());
        unionKeys.addAll(second.keySet());

        sum com = new sum();

        // Combine the two maps.
        for (String key : unionKeys)
        {
            first.put(key, com.combine((Number)first.get(key), (Number)second.get(key)));
        }
        
        return first;
    }
}
