package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.data.Period;

import java.util.Comparator;

/**
 * Class used to compare Periods based on their granularities.
 */
public class PeriodComparator implements Comparator<Period> {

    @Override
    public int compare(Period o1, Period o2) {
        if (o1.getGranularityLevel() < o2.getGranularityLevel()) {
            return -1;
        } else if (o1.getGranularityLevel() > o2.getGranularityLevel()) {
            return 1;
        } else {
            return 0;
        }

    }
}
