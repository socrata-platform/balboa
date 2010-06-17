package com.socrata.balboa.measurements.combining;

import com.socrata.balboa.metrics.measurements.combining.sum;
import junit.framework.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

public class sumTest
{
    sum sum = new sum();

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
}
