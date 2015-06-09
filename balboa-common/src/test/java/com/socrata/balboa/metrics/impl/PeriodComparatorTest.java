package com.socrata.balboa.metrics.impl;

import com.socrata.balboa.metrics.data.Period;
import com.socrata.balboa.metrics.data.impl.PeriodComparator;
import junit.framework.Assert;
import org.junit.Test;

/**
 * Unit test for comparator.
 */
public class PeriodComparatorTest {

    @Test
    public void testPeriodShouldHaveHigherGranularity() {
        Assert.assertTrue("Minute period should have higher granularity then hour.", new PeriodComparator().compare(Period.MINUTELY, Period.HOURLY) > 0);
    }

    @Test
    public void testPeriodShouldHaveLowerGranularity() {
        Assert.assertTrue("Minute period should have higher granularity then hour.", new PeriodComparator().compare(Period.DAILY, Period.HOURLY) < 0);
    }

    @Test
    public void testPeriodShouldHaveEqualGranularity() {
        Assert.assertTrue("Minute period should have higher granularity then hour.", new PeriodComparator().compare(Period.HOURLY, Period.HOURLY) == 0);
    }

}
