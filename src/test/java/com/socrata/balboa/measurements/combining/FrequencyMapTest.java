package com.socrata.balboa.measurements.combining;

import com.socrata.balboa.metrics.measurements.combining.FrequencyMap;
import junit.framework.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class FrequencyMapTest
{
    @Test
    public void testFirstNull() throws Exception
    {
        FrequencyMap m = new FrequencyMap();
        Map<String, Number> second = new HashMap<String, Number>();
        second.put("http://socrata", 10);
        Map<String, Number> results = m.combine(null, second);

        Assert.assertEquals(10, results.get("http://socrata"));
    }

    @Test
    public void testSecondNull() throws Exception
    {
        FrequencyMap m = new FrequencyMap();
        Map<String, Number> first = new HashMap<String, Number>();
        first.put("http://socrata", 10);
        Map<String, Number> results = m.combine(first, null);

        Assert.assertEquals(10, results.get("http://socrata"));
    }

    @Test
    public void testMergeAdds() throws Exception
    {
        FrequencyMap m = new FrequencyMap();
        Map<String, Number> first = new HashMap<String, Number>();
        Map<String, Number> second = new HashMap<String, Number>();

        first.put("test", 1);
        second.put("test", 2);
        second.put("foo", 5);

         Map<String, Number> results = m.combine(first, second);

        Assert.assertEquals(3, results.get("test"));
        Assert.assertEquals(5, results.get("foo"));
    }
}
