package com.socrata.balboa.metrics.data;

import org.junit.Assert;
import org.junit.Test;

import java.util.Calendar;
import java.util.GregorianCalendar;

public class DateRangeTest
{
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
