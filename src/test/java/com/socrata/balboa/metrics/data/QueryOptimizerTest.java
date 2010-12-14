package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.config.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class QueryOptimizerTest
{
    @Test
    public void testRangeLessThanOneHourReturnsOneHour() throws Exception
    {
        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        cal.set(2010, 1, 1, 1, 50);
        Date start = cal.getTime();

        cal.set(2010, 1, 1, 1, 55);
        Date end = cal.getTime();

        QueryOptimizer o  = new QueryOptimizer();
        Map<DateRange.Period, List<DateRange>> result = o.optimalSlices(start, end);
        Assert.assertEquals(1, result.size());
    }

    @Test
    public void testRangeLessThanADayButMoreThanAnHour() throws Exception
    {
        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        cal.set(2010, 1, 1, 1, 50);
        Date start = cal.getTime();

        cal.set(2010, 1, 1, 3, 55);
        Date end = cal.getTime();

        QueryOptimizer o  = new QueryOptimizer();
        Map<DateRange.Period, List<DateRange>> result = o.optimalSlices(start, end);
        
        Assert.assertTrue(result.containsKey(DateRange.Period.HOURLY));
        Assert.assertEquals(1, result.get(DateRange.Period.HOURLY).size());

        cal.set(2010, 1, 1, 1, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);

        Assert.assertEquals(cal.getTime(), result.get(DateRange.Period.HOURLY).get(0).start);

        cal.set(2010, 1, 1, 3, 59, 59);
        cal.set(Calendar.MILLISECOND, 999);
        Assert.assertEquals(cal.getTime(), result.get(DateRange.Period.HOURLY).get(0).end);
    }

    @Test
    public void testRangeLessThanADayButSpanningMoreThanADay() throws Exception
    {
        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        cal.set(2010, 1, 1, 23, 50);
        Date start = cal.getTime();

        cal.set(2010, 1, 2, 1, 55);
        Date end = cal.getTime();

        QueryOptimizer o  = new QueryOptimizer();
        Map<DateRange.Period, List<DateRange>> result = o.optimalSlices(start, end);

        Assert.assertTrue(result.containsKey(DateRange.Period.HOURLY));
        Assert.assertEquals(1, result.get(DateRange.Period.HOURLY).size());

        Assert.assertFalse(result.containsKey(DateRange.Period.DAILY));
    }

    @Test
    public void testRangeSpansMonths() throws Exception
    {
        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        cal.set(2010, 0, 30, 23, 50);
        Date start = cal.getTime();

        cal.set(2010, 6, 1, 0, 55);
        Date end = cal.getTime();

        QueryOptimizer o  = new QueryOptimizer();
        Map<DateRange.Period, List<DateRange>> result = o.optimalSlices(start, end);

        Assert.assertTrue(result.containsKey(DateRange.Period.HOURLY));
        Assert.assertEquals(2, result.get(DateRange.Period.HOURLY).size());

        Assert.assertTrue(result.containsKey(DateRange.Period.DAILY));
        Assert.assertEquals(1, result.get(DateRange.Period.DAILY).size());

        Assert.assertTrue(result.containsKey(DateRange.Period.MONTHLY));
        Assert.assertEquals(1, result.get(DateRange.Period.MONTHLY).size());

        cal.set(2010, 1, 1, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(cal.getTime(), result.get(DateRange.Period.MONTHLY).get(0).start);

        cal.set(2010, 5, 30, 23, 59, 59);
        cal.set(Calendar.MILLISECOND, 999);
        Assert.assertEquals(cal.getTime(), result.get(DateRange.Period.MONTHLY).get(0).end);
    }

    @Test
    public void testRangeSpansYears() throws Exception
    {
        List<DateRange.Period> originalPeriods = Configuration.get().getSupportedTypes();
        Configuration.get().setSupportedTypes(Arrays.asList(
                DateRange.Period.HOURLY, DateRange.Period.DAILY, DateRange.Period.MONTHLY, DateRange.Period.YEARLY
        ));

        try
        {
            Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
            cal.set(2010, 0, 30, 23, 50);
            Date start = cal.getTime();

            cal.set(2012, 6, 1, 0, 55);
            Date end = cal.getTime();

            QueryOptimizer o  = new QueryOptimizer();
            Map<DateRange.Period, List<DateRange>> result = o.optimalSlices(start, end);

            Assert.assertTrue(result.containsKey(DateRange.Period.HOURLY));
            Assert.assertEquals(2, result.get(DateRange.Period.HOURLY).size());

            Assert.assertTrue(result.containsKey(DateRange.Period.DAILY));
            Assert.assertEquals(1, result.get(DateRange.Period.DAILY).size());

            Assert.assertTrue(result.containsKey(DateRange.Period.MONTHLY));
            Assert.assertEquals(2, result.get(DateRange.Period.MONTHLY).size());

            Assert.assertTrue(result.containsKey(DateRange.Period.YEARLY));
            Assert.assertEquals(1, result.get(DateRange.Period.YEARLY).size());
        }
        finally
        {
            Configuration.get().setSupportedTypes(originalPeriods);
        }
    }

    @Test
    public void testRangeSpansYearsButDoesntIncludeYears() throws Exception
    {
        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        cal.set(2010, 0, 30, 23, 50);
        Date start = cal.getTime();

        cal.set(2011, 6, 1, 0, 55);
        Date end = cal.getTime();

        QueryOptimizer o  = new QueryOptimizer();
        Map<DateRange.Period, List<DateRange>> result = o.optimalSlices(start, end);

        Assert.assertTrue(result.containsKey(DateRange.Period.HOURLY));
        Assert.assertEquals(2, result.get(DateRange.Period.HOURLY).size());

        Assert.assertTrue(result.containsKey(DateRange.Period.DAILY));
        Assert.assertEquals(1, result.get(DateRange.Period.DAILY).size());

        Assert.assertTrue(result.containsKey(DateRange.Period.MONTHLY));
        Assert.assertEquals(1, result.get(DateRange.Period.MONTHLY).size());

        Assert.assertFalse(result.containsKey(DateRange.Period.YEARLY));
    }

    @Test
    public void testRangeExhaustivelyLookAtAllTheRangesToMakeSureTheyreRight() throws Exception
    {
        List<DateRange.Period> originalPeriods = Configuration.get().getSupportedTypes();
        Configuration.get().setSupportedTypes(Arrays.asList(
                DateRange.Period.HOURLY, DateRange.Period.DAILY, DateRange.Period.MONTHLY, DateRange.Period.YEARLY
        ));

        try
        {
            Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
            cal.set(2010, 0, 30, 23, 50);
            Date start = cal.getTime();

            cal.set(2013, 6, 1, 0, 55);
            Date end = cal.getTime();

            QueryOptimizer o  = new QueryOptimizer();
            Map<DateRange.Period, List<DateRange>> result = o.optimalSlices(start, end);

            Assert.assertTrue(result.containsKey(DateRange.Period.HOURLY));
            Assert.assertEquals(2, result.get(DateRange.Period.HOURLY).size());

            Assert.assertTrue(result.containsKey(DateRange.Period.DAILY));
            Assert.assertEquals(1, result.get(DateRange.Period.DAILY).size());

            Assert.assertTrue(result.containsKey(DateRange.Period.MONTHLY));
            Assert.assertEquals(2, result.get(DateRange.Period.MONTHLY).size());

            Assert.assertTrue(result.containsKey(DateRange.Period.YEARLY));
            Assert.assertEquals(1, result.get(DateRange.Period.YEARLY).size());

            // First check that the hourly is correct. We should have the final hour
            // of Jan 30th, 2010 and the first hour of May 1st, 2013
            cal.set(2010, 0, 30, 23, 50);
            DateRange h1 = DateRange.create(DateRange.Period.HOURLY, cal.getTime());

            cal.set(2013, 6, 1, 0, 55);
            DateRange h2 = DateRange.create(DateRange.Period.HOURLY, cal.getTime());

            List<DateRange> hours = result.get(DateRange.Period.HOURLY);

            Assert.assertTrue(hours.contains(h1));
            Assert.assertTrue(hours.contains(h2));

            // Next check that the daily is correct. We should have only one day:
            // Jan 31st, 2010.
            cal.set(2010, 0, 31, 1, 0);
            DateRange d1 = DateRange.create(DateRange.Period.DAILY, cal.getTime());

            List<DateRange> days = result.get(DateRange.Period.DAILY);

            Assert.assertTrue(days.contains(d1));

            // Now check that the monthly is correct. We should have 17 months that
            // are listed contiguously:
            //
            // For the "up":
            // Feb 2010, Mar 2010, Apr 2010, May 2010, Jun 2010, Jul 2010, Aug 2010,
            // Sep 2010, Oct 2010, Nov 2010, Dec 2010
            //
            // For the "down":
            // Jun 2013, May 2013, Apr 2013, Mar 2013, Feb 2013, Jan 2013
            cal.set(2010, 1, 1, 0, 0, 0);
            cal.set(Calendar.MILLISECOND, 0);
            Date s = cal.getTime();

            cal.set(2010, 11, 31, 23, 59, 59);
            cal.set(Calendar.MILLISECOND, 999);
            Date e = cal.getTime();
            DateRange m1 = new DateRange(s, e);

            Assert.assertTrue(result.get(DateRange.Period.MONTHLY).contains(m1));

            cal.set(2013, 0, 1, 0, 0, 0);
            cal.set(Calendar.MILLISECOND, 0);
            s = cal.getTime();

            cal.set(2013, 5, 30, 23, 59, 59);
            cal.set(Calendar.MILLISECOND, 999);
            e = cal.getTime();
            DateRange m2 = new DateRange(s, e);

            Assert.assertTrue(result.get(DateRange.Period.MONTHLY).contains(m2));

            // Finally we just have two years that we have to verify. 2011, and 2012
            cal.set(2011, 0, 1);
            DateRange y1 = DateRange.create(DateRange.Period.YEARLY, cal.getTime());

            cal.set(2012, 0, 1);
            DateRange y2 = DateRange.create(DateRange.Period.YEARLY, cal.getTime());

            DateRange ys = new DateRange(y1.start, y2.end);

            List<DateRange> years = result.get(DateRange.Period.YEARLY);
            Assert.assertTrue(years.contains(ys));
        }
        finally
        {
            Configuration.get().setSupportedTypes(originalPeriods);
        }
    }
}
