package com.socrata.balboa.server;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import com.socrata.balboa.metrics.data.DateRange;
import org.junit.Test;
import org.junit.Assert;

import java.util.*;

public class MetricsServiceTest
{
    @Test
    public void testRangeLessThanOneHourReturnsOneHour() throws Exception
    {
        Calendar cal = new GregorianCalendar();
        cal.set(2010, 1, 1, 1, 50);
        Date start = cal.getTime();

        cal.set(2010, 1, 1, 1, 55);
        Date end = cal.getTime();

        MetricsService service = new MetricsService();
        Map<Summary.Type, List<DateRange>> result = service.optimalSlices(start, end);
        Assert.assertEquals(1, result.size());
    }

    @Test
    public void testRangeLessThanADayButMoreThanAnHour() throws Exception
    {
        Calendar cal = new GregorianCalendar();
        cal.set(2010, 1, 1, 1, 50);
        Date start = cal.getTime();

        cal.set(2010, 1, 1, 3, 55);
        Date end = cal.getTime();

        MetricsService service = new MetricsService();
        Map<Summary.Type, List<DateRange>> result = service.optimalSlices(start, end);
        
        Assert.assertTrue(result.containsKey(Summary.Type.HOURLY));
        Assert.assertEquals(3, result.get(Summary.Type.HOURLY).size());
    }

    @Test
    public void testRangeLessThanADayButSpanningMoreThanADay() throws Exception
    {
        Calendar cal = new GregorianCalendar();
        cal.set(2010, 1, 1, 23, 50);
        Date start = cal.getTime();

        cal.set(2010, 1, 2, 1, 55);
        Date end = cal.getTime();

        MetricsService service = new MetricsService();
        Map<Summary.Type, List<DateRange>> result = service.optimalSlices(start, end);

        Assert.assertTrue(result.containsKey(Summary.Type.HOURLY));
        Assert.assertEquals(3, result.get(Summary.Type.HOURLY).size());

        Assert.assertFalse(result.containsKey(Summary.Type.DAILY));
    }

    @Test
    public void testRangeSpansMonths() throws Exception
    {
        Calendar cal = new GregorianCalendar();
        cal.set(2010, 0, 30, 23, 50);
        Date start = cal.getTime();

        cal.set(2010, 6, 1, 0, 55);
        Date end = cal.getTime();

        MetricsService service = new MetricsService();
        Map<Summary.Type, List<DateRange>> result = service.optimalSlices(start, end);

        Assert.assertTrue(result.containsKey(Summary.Type.HOURLY));
        Assert.assertEquals(2, result.get(Summary.Type.HOURLY).size());

        Assert.assertTrue(result.containsKey(Summary.Type.DAILY));
        Assert.assertEquals(1, result.get(Summary.Type.DAILY).size());

        Assert.assertTrue(result.containsKey(Summary.Type.MONTHLY));
        Assert.assertEquals(5, result.get(Summary.Type.MONTHLY).size());
    }

    @Test
    public void testRangeSpansYears() throws Exception
    {
        Calendar cal = new GregorianCalendar();
        cal.set(2010, 0, 30, 23, 50);
        Date start = cal.getTime();

        cal.set(2012, 6, 1, 0, 55);
        Date end = cal.getTime();

        MetricsService service = new MetricsService();
        Map<Summary.Type, List<DateRange>> result = service.optimalSlices(start, end);

        Assert.assertTrue(result.containsKey(Summary.Type.HOURLY));
        Assert.assertEquals(2, result.get(Summary.Type.HOURLY).size());

        Assert.assertTrue(result.containsKey(Summary.Type.DAILY));
        Assert.assertEquals(1, result.get(Summary.Type.DAILY).size());

        Assert.assertTrue(result.containsKey(Summary.Type.MONTHLY));
        Assert.assertEquals(17, result.get(Summary.Type.MONTHLY).size());

        Assert.assertTrue(result.containsKey(Summary.Type.YEARLY));
        Assert.assertEquals(1, result.get(Summary.Type.YEARLY).size());
    }

    @Test
    public void testRangeSpansYearsButDoesntIncludeYears() throws Exception
    {
        Calendar cal = new GregorianCalendar();
        cal.set(2010, 0, 30, 23, 50);
        Date start = cal.getTime();

        cal.set(2011, 6, 1, 0, 55);
        Date end = cal.getTime();

        MetricsService service = new MetricsService();
        Map<Summary.Type, List<DateRange>> result = service.optimalSlices(start, end);

        Assert.assertTrue(result.containsKey(Summary.Type.HOURLY));
        Assert.assertEquals(2, result.get(Summary.Type.HOURLY).size());

        Assert.assertTrue(result.containsKey(Summary.Type.DAILY));
        Assert.assertEquals(1, result.get(Summary.Type.DAILY).size());

        Assert.assertTrue(result.containsKey(Summary.Type.MONTHLY));
        Assert.assertEquals(17, result.get(Summary.Type.MONTHLY).size());
    }

    @Test
    public void testRangeExhaustivelyLookAtAllTheRangesToMakeSureTheyreRight() throws Exception
    {
        Calendar cal = new GregorianCalendar();
        cal.set(2010, 0, 30, 23, 50);
        Date start = cal.getTime();

        cal.set(2013, 6, 1, 0, 55);
        Date end = cal.getTime();

        MetricsService service = new MetricsService();
        Map<Summary.Type, List<DateRange>> result = service.optimalSlices(start, end);

        Assert.assertTrue(result.containsKey(Summary.Type.HOURLY));
        Assert.assertEquals(2, result.get(Summary.Type.HOURLY).size());

        Assert.assertTrue(result.containsKey(Summary.Type.DAILY));
        Assert.assertEquals(1, result.get(Summary.Type.DAILY).size());

        Assert.assertTrue(result.containsKey(Summary.Type.MONTHLY));
        Assert.assertEquals(17, result.get(Summary.Type.MONTHLY).size());

        Assert.assertTrue(result.containsKey(Summary.Type.YEARLY));
        Assert.assertEquals(2, result.get(Summary.Type.YEARLY).size());

        // First check that the hourly is correct. We should have the final hour
        // of Jan 30th, 2010 and the first hour of May 1st, 2013
        cal.set(2010, 0, 30, 23, 50);
        DateRange h1 = DateRange.create(Summary.Type.HOURLY, cal.getTime());

        cal.set(2013, 6, 1, 0, 55);
        DateRange h2 = DateRange.create(Summary.Type.HOURLY, cal.getTime());

        List<DateRange> hours = result.get(Summary.Type.HOURLY);

        Assert.assertTrue(hours.contains(h1));
        Assert.assertTrue(hours.contains(h2));

        // Next check that the daily is correct. We should have only one day:
        // Jan 31st, 2010.
        cal.set(2010, 0, 31, 1, 0);
        DateRange d1 = DateRange.create(Summary.Type.DAILY, cal.getTime());

        List<DateRange> days = result.get(Summary.Type.DAILY);

        Assert.assertTrue(days.contains(d1));

        // Now check that the monthly is correct. We should have 17 months:
        //
        // For the "up":
        // Feb 2010, Mar 2010, Apr 2010, May 2010, Jun 2010, Jul 2010, Aug 2010,
        // Sep 2010, Oct 2010, Nov 2010, Dec 2010
        //
        // For the "down":
        // Jun 2013, May 2013, Apr 2013, Mar 2013, Feb 2013, Jan 2013
        List<DateRange> ups = new ArrayList<DateRange>(11);
        for (int i=1 /* February */; i < 12 /* December + 1 */; i++)
        {
            cal.set(2010, i, 1);
            ups.add(DateRange.create(Summary.Type.MONTHLY, cal.getTime()));
        }

        List<DateRange> downs = new ArrayList<DateRange>(6);
        for (int i=0 /* January */; i < 6 /* June + 1 */; i++)
        {
            cal.set(2013, i, 1);
            downs.add(DateRange.create(Summary.Type.MONTHLY, cal.getTime()));
        }

        // And verify...
        List<DateRange> months = result.get(Summary.Type.MONTHLY);
        for (DateRange r : ups)
        {
            Assert.assertTrue(months.contains(r));
        }

        for (DateRange r : downs)
        {
            Assert.assertTrue(months.contains(r));
        }

        // Finally we just have two years that we have to verify. 2011, and 2012
        cal.set(2011, 0, 1);
        DateRange y1 = DateRange.create(Summary.Type.YEARLY, cal.getTime());

        cal.set(2012, 0, 1);
        DateRange y2 = DateRange.create(Summary.Type.YEARLY, cal.getTime());

        List<DateRange> years = result.get(Summary.Type.YEARLY);
        Assert.assertTrue(years.contains(y1));
        Assert.assertTrue(years.contains(y2));
    }
}
