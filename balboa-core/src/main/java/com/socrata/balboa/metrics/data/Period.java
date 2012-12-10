package com.socrata.balboa.metrics.data;

import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public enum Period {
    FOREVER,
    YEARLY,
    MONTHLY,
    WEEKLY,
    DAILY,
    HOURLY,
    FIFTEEN_MINUTE,
    MINUTELY,
    SECONDLY,
    REALTIME;

    @Override
    public String toString() {
        return this.name().toLowerCase();
    }

    public static Period leastGranular(Collection<Period> col) {
        return Collections.min(col);
    }

    public static Period mostGranular(Collection<Period> col) {
        return Collections.max(col);
    }

    /**
     * Retrieve the adjacent type that is less granular that the current.
     * For example, "day" is less granular than "hour" which is slightly
     * less granular than "minute".
     */
    public Period lessGranular() {
        switch (this) {
            case REALTIME:
                return SECONDLY;
            case SECONDLY:
                return MINUTELY;
            case MINUTELY:
                return FIFTEEN_MINUTE;
            case FIFTEEN_MINUTE:
                return HOURLY;
            case HOURLY:
                return DAILY;
            case DAILY:
            case WEEKLY:
                return MONTHLY;
            case MONTHLY:
                return YEARLY;
            case YEARLY:
                return FOREVER;
            default:
                return null;
        }
    }

    /**
     * Retrieve the adjacent type that is more granular than the current.
     * For example, "day" is slightly more granular than "month" which is
     * slightly more granular than "year".
     */
    public Period moreGranular() {
        switch (this) {
            case FOREVER:
                return YEARLY;
            case YEARLY:
                return MONTHLY;
            case MONTHLY:
            case WEEKLY:
                // Because weeks don't fall on month boundaries we can't
                // summarize them as the next best of months.
                return DAILY;
            case DAILY:
                return HOURLY;
            case HOURLY:
                return FIFTEEN_MINUTE;
            case FIFTEEN_MINUTE:
                return MINUTELY;
            case MINUTELY:
                return SECONDLY;
            case SECONDLY:
                return REALTIME;
            default:
                return null;
        }
    }

}
