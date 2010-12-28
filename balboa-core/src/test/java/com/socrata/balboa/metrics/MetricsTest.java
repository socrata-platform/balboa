package com.socrata.balboa.metrics;

import junit.framework.Assert;
import org.junit.Test;

public class MetricsTest
{
    @Test
    public void testMerge() throws Exception
    {
        Metrics m1 = new Metrics();
        Metrics m2 = new Metrics();
        m2.put("hello", new Metric(Metric.RecordType.AGGREGATE, 1));

        m1.merge(m2);

        Assert.assertTrue(m1.containsKey("hello"));
        Assert.assertEquals(1, m1.get("hello").getValue());

        m1.merge(m2);
        Assert.assertEquals(2, m1.get("hello").getValue());
    }

    @Test
    public void testMerge2() throws Exception
    {
        Metrics m1 = new Metrics();
        m1.put("hello", new Metric(Metric.RecordType.AGGREGATE, 55));
        Metrics m2 = new Metrics();
        m2.put("hello", new Metric(Metric.RecordType.AGGREGATE, 1));

        m1.merge(m2);

        Assert.assertTrue(m1.containsKey("hello"));
        Assert.assertEquals(56, m1.get("hello").getValue());

        m1.merge(m2);
        Assert.assertEquals(57, m1.get("hello").getValue());
    }

    @Test
    public void testMerge3() throws Exception
    {
        Metrics m1 = new Metrics();
        m1.put("hello", new Metric(Metric.RecordType.AGGREGATE, 55));
        m1.put("hello2", new Metric(Metric.RecordType.AGGREGATE, 55));
        Metrics m2 = new Metrics();
        m2.put("hello", new Metric(Metric.RecordType.AGGREGATE, 1));

        m1.merge(m2);

        Assert.assertTrue(m1.containsKey("hello"));
        Assert.assertEquals(56, m1.get("hello").getValue());
        Assert.assertTrue(m1.containsKey("hello2"));
        Assert.assertEquals(55, m1.get("hello2").getValue());
    }

    @Test(expected=IllegalArgumentException.class)
    public void testMismatchedTypes() throws Exception
    {
        Metrics m1 = new Metrics();
        m1.put("hello", new Metric(Metric.RecordType.AGGREGATE, 55));
        Metrics m2 = new Metrics();
        m2.put("hello", new Metric(Metric.RecordType.ABSOLUTE, 10));

        m1.merge(m2);
    }

    @Test
    public void testMergeNegatives() throws Exception
    {
        Metrics m1 = new Metrics();
        Metrics m2 = new Metrics();
        m2.put("hello", new Metric(Metric.RecordType.AGGREGATE, -1));

        m1.merge(m2);

        Assert.assertTrue(m1.containsKey("hello"));
        Assert.assertEquals(-1, m1.get("hello").getValue());

        m1.merge(m2);
        Assert.assertEquals(-2l, m1.get("hello").getValue());
    }

    @Test
    public void testMergeAbsolute() throws Exception
    {
        Metrics m1 = new Metrics();
        Metrics m2 = new Metrics();
        m2.put("hello", new Metric(Metric.RecordType.ABSOLUTE, 1));

        m1.merge(m2);

        Assert.assertTrue(m1.containsKey("hello"));
        Assert.assertEquals(1, m1.get("hello").getValue());

        m1.merge(m2);
        Assert.assertEquals(1, m1.get("hello").getValue());
    }
}
