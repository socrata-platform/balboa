package com.socrata.balboa.metrics.data;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class DateRangeTest {
    @Test
    public void testLeastGranular() throws Exception {
        List<Period> periods = Arrays.asList(Period.values());

        Period t = Period.leastGranular(periods);

        Assert.assertEquals(Period.FOREVER, t);
    }

    @Test
    public void testMostGranular() throws Exception {
        List<Period> periods = Arrays.asList(Period.values());

        Period t = Period.mostGranular(periods);

        Assert.assertEquals(Period.REALTIME, t);
    }

    @Test
    public void testEquals() throws Exception {
        DateRange r1 = DateRange.create(Period.MONTHLY, new Date(0));
        DateRange r2 = DateRange.create(Period.MONTHLY, new Date(0));

        Assert.assertEquals(r1, r2);
        r2.end = new Date(r2.end.getTime() + 1);
        Assert.assertFalse(r1.equals(r2));
        r2.start = new Date(r2.start.getTime() + 1);
        Assert.assertFalse(r1.equals(r2));
    }

    @Test
    public void testInclude() throws Exception {
        DateRange range = DateRange.create(Period.MONTHLY, new Date(0));

        Assert.assertFalse(range.includes(new Date(range.start.getTime() - 1)));
        Assert.assertTrue(range.includes(range.start));
        Assert.assertTrue(range.includes(range.end));
        Assert.assertTrue(range.includes(new Date(range.start.getTime() + 1)));
        Assert.assertFalse(range.includes(new Date(range.end.getTime() + 1)));
    }

    @Test
    public void testSecondly() throws Exception {
        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        cal.set(2010, 1, 12, 16, 59, 12);

        DateRange range = DateRange.createSecondly(cal.getTime());

        cal.set(2010, 1, 12, 16, 59, 12);
        cal.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(cal.getTime(), range.start);

        cal.set(2010, 1, 12, 16, 59, 12);
        cal.set(Calendar.MILLISECOND, 999);
        Assert.assertEquals(cal.getTime(), range.end);
    }

    @Test
    public void testMinutely() throws Exception {
        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        cal.set(2010, 1, 12, 16, 59, 12);

        DateRange range = DateRange.createMinutely(cal.getTime());

        cal.set(2010, 1, 12, 16, 59, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(cal.getTime(), range.start);

        cal.set(2010, 1, 12, 16, 59, 59);
        cal.set(Calendar.MILLISECOND, 999);
        Assert.assertEquals(cal.getTime(), range.end);
    }

    @Test
    public void testForever() throws Exception {
        DateRange range = DateRange.createForever(new Date(0));
    }

    @Test
    public void testDatRangeStartCantBeBeforeEnd() throws Exception {
        Calendar start = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        start.set(2010, 1, 1);

        Calendar end = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        end.set(2009, 1, 1);

        try {
            new DateRange(start.getTime(), end.getTime());
            Assert.fail("Invalid date range was constructed.");
        } catch (IllegalArgumentException e) {
        }
    }


    @Test
    public void testFifteenMinutely() throws Exception {
        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        cal.set(2010, 1, 12, 16, 59, 12);

        DateRange range = DateRange.createFifteenMinutely(cal.getTime());

        cal.set(2010, 1, 12, 16, 45, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(cal.getTime(), range.start);

        cal.set(2010, 1, 12, 16, 59, 59);
        cal.set(Calendar.MILLISECOND, 999);
        Assert.assertEquals(cal.getTime(), range.end);

        cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        cal.set(2010, 1, 12, 16, 34, 12);

        range = DateRange.createFifteenMinutely(cal.getTime());

        cal.set(2010, 1, 12, 16, 30, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(cal.getTime(), range.start);

        cal.set(2010, 1, 12, 16, 44, 59);
        cal.set(Calendar.MILLISECOND, 999);
        Assert.assertEquals(cal.getTime(), range.end);

    }


    @Test
    public void testHourly() throws Exception {
        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
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
    public void testMonthly() throws Exception {
        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
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
    public void testWeekly() throws Exception {
        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
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
    public void testWeeklyCrossingMonthlyBorder() throws Exception {
        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
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
    public void testDaily() throws Exception {
        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
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
    public void testMonthlyOnStartingBorder() throws Exception {
        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
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
    public void testMonthlyOnEndBorder() throws Exception {
        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
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
