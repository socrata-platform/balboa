package com.socrata.balboa.metrics.data;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class DateRangeTest
{
    @Test
    public void testLeastGranular() throws Exception
    {
        List<DateRange.Type> types = Arrays.asList(DateRange.Type.values());

        DateRange.Type t = DateRange.Type.leastGranular(types);

        Assert.assertEquals(DateRange.Type.FOREVER, t);
    }

    @Test
    public void testMostGranular() throws Exception
    {
        List<DateRange.Type> types = Arrays.asList(DateRange.Type.values());

        DateRange.Type t = DateRange.Type.mostGranular(types);

        Assert.assertEquals(DateRange.Type.REALTIME, t);
    }

    @Test
    public void testForever() throws Exception
    {
        DateRange range = DateRange.createForever(new Date(0));
    }
    
    @Test
    public void testDatRangeStartCantBeBeforeEnd() throws Exception
    {
        Calendar start = new GregorianCalendar();
        start.set(2010, 1, 1);

        Calendar end = new GregorianCalendar();
        end.set(2009, 1, 1);

        try
        {
            new DateRange(start.getTime(), end.getTime());
            Assert.fail("Invalid date range was constructed.");
        }
        catch (IllegalArgumentException e) {}
    }

    @Test
    public void testHourly() throws Exception
    {
        Calendar cal = new GregorianCalendar();
        cal.set(2010, 1, 12, 16, 59, 12);

        DateRange range = DateRange.createHourly(cal.getTime());

        cal.set(2010, 1, 12, 16, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(cal.getTime(), range.start);

        cal.set(2010, 1, 12, 16, 59, 59);
        cal.set(Calendar.MILLISECOND, 999);
        Assert.assertEquals(cal.getTime(), range.end);
    }

    @Test
    public void testMonthly() throws Exception
    {
        Calendar cal = new GregorianCalendar();
        cal.set(2010, 1, 12);

        DateRange range = DateRange.createMonthly(cal.getTime());

        cal.set(2010, 1, 1, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(cal.getTime(), range.start);

        cal.set(2010, 1, 28, 23, 59, 59);
        cal.set(Calendar.MILLISECOND, 999);
        Assert.assertEquals(cal.getTime(), range.end);
    }

    @Test
    public void testWeekly() throws Exception
    {
        Calendar cal = new GregorianCalendar();
        cal.set(2010, 1, 12);

        DateRange range = DateRange.createWeekly(cal.getTime());

        cal.set(2010, 1, 7, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(cal.getTime(), range.start);

        cal.set(2010, 1, 13, 23, 59, 59);
        cal.set(Calendar.MILLISECOND, 999);
        Assert.assertEquals(cal.getTime(), range.end);
    }

    @Test
    public void testWeeklyCrossingMonthlyBorder() throws Exception
    {
        Calendar cal = new GregorianCalendar();
        cal.set(2010, 5, 2);

        DateRange range = DateRange.createWeekly(cal.getTime());

        cal.set(2010, 4, 30, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(cal.getTime(), range.start);

        cal.set(2010, 5, 5, 23, 59, 59);
        cal.set(Calendar.MILLISECOND, 999);
        Assert.assertEquals(cal.getTime(), range.end);
    }

    @Test
    public void testDaily() throws Exception
    {
        Calendar cal = new GregorianCalendar();
        cal.set(2010, 5, 2, 12, 12, 12);

        DateRange range = DateRange.createDaily(cal.getTime());

        cal.set(2010, 5, 2, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(cal.getTime(), range.start);

        cal.set(2010, 5, 2, 23, 59, 59);
        cal.set(Calendar.MILLISECOND, 999);
        Assert.assertEquals(cal.getTime(), range.end);
    }

    @Test
    public void testMonthlyOnStartingBorder() throws Exception
    {
        Calendar cal = new GregorianCalendar();
        cal.set(2010, 1, 1, 0, 0, 0);

        DateRange range = DateRange.createMonthly(cal.getTime());

        cal.set(2010, 1, 1, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(cal.getTime(), range.start);

        cal.set(2010, 1, 28, 23, 59, 59);
        cal.set(Calendar.MILLISECOND, 999);
        Assert.assertEquals(cal.getTime(), range.end);
    }

    @Test
    public void testMonthlyOnEndBorder() throws Exception
    {
        Calendar cal = new GregorianCalendar();
        cal.set(2010, 1, 28, 23, 59, 59);

        DateRange range = DateRange.createMonthly(cal.getTime());

        cal.set(2010, 1, 1, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(cal.getTime(), range.start);

        cal.set(2010, 1, 28, 23, 59, 59);
        cal.set(Calendar.MILLISECOND, 999);
        Assert.assertEquals(cal.getTime(), range.end);
    }
}
