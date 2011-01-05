package com.socrata.balboa.metrics.measurements.combining;

import junit.framework.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

public class SummationTest
{
    Summation sum = new Summation();

    @Test
    public void testShort() throws Exception
    {
        Assert.assertEquals(128, sum.combine(new Short("127"), 1));
    }

    @Test
    public void testIntegerLikeObjects() throws Exception
    {
        Assert.assertEquals(20, sum.combine(new Integer(10), new Integer(10)));

        Assert.assertEquals(20l, sum.combine(new Integer(10), new Long(10)));
        Assert.assertEquals(20l, sum.combine(new Long(10), new Integer(10)));

        Assert.assertEquals(new BigDecimal(20), sum.combine(new BigInteger("10"), 10));
        Assert.assertEquals(new BigDecimal(20), sum.combine(10, new BigInteger("10")));

        Assert.assertEquals(new BigDecimal(20), sum.combine(new BigInteger("10"), new Long(10)));
        Assert.assertEquals(new BigDecimal(20), sum.combine(new Long(10), new BigInteger("10")));

        Assert.assertEquals(new BigDecimal(20), sum.combine(new BigInteger("10"), new BigDecimal("10")));
        Assert.assertEquals(new BigDecimal(20), sum.combine(new BigDecimal("10"), new BigInteger("10")));

        Assert.assertEquals(new BigDecimal(20), sum.combine(10, new BigDecimal("10")));
        Assert.assertEquals(new BigDecimal(20), sum.combine(new BigDecimal("10"), 10));
    }

    @Test
    public void testDoubleLikeObjects() throws Exception
    {
        Assert.assertEquals(1.5d, sum.combine(new Double(1.0d), new Double(0.5d)));

        Assert.assertEquals(1.5d, sum.combine(new Float(1.0d), new Double(0.5d)));
        Assert.assertEquals(1.5d, sum.combine(new Double(0.5d), new Float(1.0d)));

        Assert.assertEquals(new BigDecimal("1.5"), sum.combine(new BigDecimal("1.0"), new Double(0.5d)));
        Assert.assertEquals(new BigDecimal("1.5"), sum.combine(new Double(0.5d), new BigDecimal("1.0")));
    }

    @Test
    public void testCombined() throws Exception
    {
        Assert.assertEquals(new BigDecimal("100.0"), sum.combine(new BigDecimal("50"), 50.0d));
        Assert.assertEquals(new BigDecimal("100.0"), sum.combine(50.0d, new BigDecimal("50")));

        Assert.assertEquals(100.0d, sum.combine(new Double("50"), 50));
        Assert.assertEquals(100.0d, sum.combine(50, new Double("50")));
    }

    @Test
    public void testSumDoublesWithOverflow() throws Exception
    {
        Number result = sum.combine(Double.MAX_VALUE, Double.MAX_VALUE);

        Assert.assertTrue(result instanceof BigDecimal);
        Assert.assertEquals(new BigDecimal(Double.MAX_VALUE).multiply(new BigDecimal(2)), result);
    }

    @Test
    public void testSumLongsWithOverflow() throws Exception
    {
        Number result = sum.combine(Long.MAX_VALUE, Long.MAX_VALUE);

        Assert.assertTrue(result instanceof BigDecimal);
        Assert.assertEquals(new BigDecimal(Long.MAX_VALUE).multiply(new BigDecimal(2)), result);
    }

    @Test
    public void testSumIntsWithOverflow() throws Exception
    {
        Number result = sum.combine(Integer.MAX_VALUE, Integer.MAX_VALUE);

        Assert.assertTrue(result instanceof Long);
        Assert.assertEquals(new Long(Integer.MAX_VALUE) * 2, result);
    }
}
