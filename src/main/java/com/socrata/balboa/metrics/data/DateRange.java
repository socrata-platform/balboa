package com.socrata.balboa.metrics.data;

import java.util.*;

public class DateRange
{
    /**
     * A type of time span.
     */
    public static enum Type
    {
        FOREVER,
        YEARLY,
        MONTHLY,
        WEEKLY,
        DAILY,
        HOURLY,
        MINUTELY,
        SECONDLY,
        REALTIME;

        @Override
        public String toString()
        {
            return this.name().toLowerCase();
        }

        public static DateRange.Type leastGranular(Collection<Type> col)
        {
            return Collections.min(col);
        }

        public static DateRange.Type mostGranular(Collection<DateRange.Type> col)
        {
            return Collections.max(col);
        }

        /**
         * Retrieve the adjacent type that is less granular that the current.
         * For example, "day" is less granular than "hour" which is slightly
         * less granular than "minute".
         */
        public Type lessGranular()
        {
            switch(this)
            {
                case REALTIME:
                    return SECONDLY;
                case SECONDLY:
                    return MINUTELY;
                case MINUTELY:
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
        public Type moreGranular()
        {
            switch(this)
            {
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
    
    public Date start;
    public Date end;

    public DateRange(Date start, Date end)
    {
        if (start.after(end))
        {
            throw new IllegalArgumentException("The start time must be before the end time '" + start + "' !< '" + end + "'.");
        }
        
        this.start = start;
        this.end = end;
    }

    /**
     * Get a date range that covers everything in the past/present/future. Kind
     * of like Timecop.
     */
    static DateRange createForever(Date date)
    {
        Calendar start = new GregorianCalendar();

        // Trim the day of the year off the requested date.
        start.set(start.getActualMinimum(Calendar.YEAR),
                  start.getActualMinimum(Calendar.MONTH),
                  start.getActualMinimum(Calendar.DATE),
                  start.getActualMinimum(Calendar.HOUR_OF_DAY),
                  start.getActualMinimum(Calendar.MINUTE),
                  start.getActualMinimum(Calendar.SECOND));
        start.set(Calendar.MILLISECOND, 0);

        Calendar end = new GregorianCalendar();
        end.setTime(date);

        // Set the day to the end of the year of the requested date.
        end.set(end.getActualMaximum(Calendar.YEAR) - 1,
                end.getActualMaximum(Calendar.MONTH),
                end.getActualMaximum(Calendar.DATE),
                end.getActualMaximum(Calendar.HOUR_OF_DAY),
                end.getActualMaximum(Calendar.MINUTE),
                end.getActualMaximum(Calendar.SECOND));
        end.set(Calendar.MILLISECOND, 999);

        return new DateRange(start.getTime(), end.getTime());
    }

    /**
     * Create a yearly date range for a given date for the year to which
     * the date belongs. For example, if the input date is "2010-05-28", the
     * range returned would be (2010-01-01 -> 2010-12-31).
     */
    static DateRange createYearly(Date date)
    {
        Calendar start = new GregorianCalendar();
        start.setTime(date);

        // Trim the day of the year off the requested date.
        start.set(start.get(Calendar.YEAR),
                  start.getActualMinimum(Calendar.MONTH),
                  start.getActualMinimum(Calendar.DATE),
                  start.getActualMinimum(Calendar.HOUR_OF_DAY),
                  start.getActualMinimum(Calendar.MINUTE),
                  start.getActualMinimum(Calendar.SECOND));
        start.set(Calendar.MILLISECOND, 0);

        Calendar end = new GregorianCalendar();
        end.setTime(date);

        // Obnoxiously, before we can fetch the maximum number of days in a
        // month we have to first set the calendar to that month (since each
        // month has a different number of days). So, first set the calendar
        // to december, then do the normal getActualMaximum spiel.
        end.set(end.get(Calendar.YEAR),
                end.getActualMaximum(Calendar.MONTH),
                1);

        // Set the day to the end of the year of the requested date.
        end.set(end.get(Calendar.YEAR),
                end.getActualMaximum(Calendar.MONTH),
                end.getActualMaximum(Calendar.DATE),
                end.getActualMaximum(Calendar.HOUR_OF_DAY),
                end.getActualMaximum(Calendar.MINUTE),
                end.getActualMaximum(Calendar.SECOND));
        end.set(Calendar.MILLISECOND, 999);

        return new DateRange(start.getTime(), end.getTime());
    }

    /**
     * Create a monthly date range for a given date for the month to which
     * the date belongs. For example, if the input date is "2010-05-28", the
     * range returned would be (2010-05-01 -> 2010-05-31).
     */
    static DateRange createMonthly(Date date)
    {
        Calendar start = new GregorianCalendar();
        start.setTime(date);

        // Trim the day of the month off the requested date.
        start.set(start.get(Calendar.YEAR),
                  start.get(Calendar.MONTH),
                  start.getActualMinimum(Calendar.DATE),
                  start.getActualMinimum(Calendar.HOUR_OF_DAY),
                  start.getActualMinimum(Calendar.MINUTE),
                  start.getActualMinimum(Calendar.SECOND));
        start.set(Calendar.MILLISECOND, 0);

        Calendar end = new GregorianCalendar();
        end.setTime(date);

        // Set the day to the end of the month of the requested date.
        end.set(end.get(Calendar.YEAR),
                end.get(Calendar.MONTH),
                end.getActualMaximum(Calendar.DATE),
                end.getActualMaximum(Calendar.HOUR_OF_DAY),
                end.getActualMaximum(Calendar.MINUTE),
                end.getActualMaximum(Calendar.SECOND));
        end.set(Calendar.MILLISECOND, 999);

        return new DateRange(start.getTime(), end.getTime());
    }

    /**
     * Create a weekly date range for a given date for the month to which
     * the date belongs. For example if the date is the wednesday of any
     * given week, the date range would look like (previous sunday -> next
     * saturday) encompassing all 7 days of the week.
     */
    static DateRange createWeekly(Date date)
    {
        Calendar start = new GregorianCalendar();
        start.setTime(date);

        // Set the day to the beginning of the week of the reqeusted date.
        start.set(start.get(Calendar.YEAR),
                  start.get(Calendar.MONTH),
                  start.get(Calendar.DATE) - start.get(Calendar.DAY_OF_WEEK) + 1,
                  start.getActualMinimum(Calendar.HOUR_OF_DAY),
                  start.getActualMinimum(Calendar.MINUTE),
                  start.getActualMinimum(Calendar.SECOND));
        start.set(Calendar.MILLISECOND, 0);

        Calendar end = new GregorianCalendar();
        end.setTime(date);

        // Set the day to the end of the week of the requested date.
        end.set(end.get(Calendar.YEAR),
                end.get(Calendar.MONTH),
                end.get(Calendar.DATE) + (end.getActualMaximum(Calendar.DAY_OF_WEEK) - end.get(Calendar.DAY_OF_WEEK)),
                end.getActualMaximum(Calendar.HOUR_OF_DAY),
                end.getActualMaximum(Calendar.MINUTE),
                end.getActualMaximum(Calendar.SECOND));
        end.set(Calendar.MILLISECOND, 999);

        return new DateRange(start.getTime(), end.getTime());
    }

    /**
     * Create a daily date range for a given date for the month to which
     * the date belongs. So if the date is "2010-05-28 16:14:08" then the
     * returned range would be (2010-05-28 00:00:00 -> 2010-05-28 23:59:59).
     */
    static DateRange createDaily(Date date)
    {
        Calendar start = new GregorianCalendar();
        start.setTime(date);

        // Set the day to the beginning of the day of the reqeusted date.
        start.set(start.get(Calendar.YEAR),
                  start.get(Calendar.MONTH),
                  start.get(Calendar.DATE),
                  start.getActualMinimum(Calendar.HOUR_OF_DAY),
                  start.getActualMinimum(Calendar.MINUTE),
                  start.getActualMinimum(Calendar.SECOND));
        start.set(Calendar.MILLISECOND, 0);

        Calendar end = new GregorianCalendar();
        end.setTime(date);

        // Set the day to the end of the day of the requested date.
        end.set(end.get(Calendar.YEAR),
                end.get(Calendar.MONTH),
                end.get(Calendar.DATE),
                end.getActualMaximum(Calendar.HOUR_OF_DAY),
                end.getActualMaximum(Calendar.MINUTE),
                end.getActualMaximum(Calendar.SECOND));
        end.set(Calendar.MILLISECOND, 999);

        return new DateRange(start.getTime(), end.getTime());
    }

    /**
     * Create an hourly date range for a given day to which the date belongs.
     * So if the date is "2010-05-28 16:14:08" then the returned range would be
     * (2010-05-28 16:00:00 -> 2010-05-28 16:59:59).
     */
    static DateRange createHourly(Date date)
    {
        Calendar start = new GregorianCalendar();
        start.setTime(date);

        // Set the time to the beginning of the hour of the requested date.
        start.set(start.get(Calendar.YEAR),
                  start.get(Calendar.MONTH),
                  start.get(Calendar.DATE),
                  start.get(Calendar.HOUR_OF_DAY),
                  start.getActualMinimum(Calendar.MINUTE),
                  start.getActualMinimum(Calendar.SECOND));
        start.set(Calendar.MILLISECOND, 0);

        Calendar end = new GregorianCalendar();
        end.setTime(date);

        // Set the day to the end of the day of the requested date.
        end.set(end.get(Calendar.YEAR),
                end.get(Calendar.MONTH),
                end.get(Calendar.DATE),
                end.get(Calendar.HOUR_OF_DAY),
                end.getActualMaximum(Calendar.MINUTE),
                end.getActualMaximum(Calendar.SECOND));
        end.set(Calendar.MILLISECOND, 999);

        return new DateRange(start.getTime(), end.getTime());
    }

    /**
     * Create a minute range for a given time. So if the date is
     * "2010-05-28 16:14:08" then the returned range would be
     * (2010-05-28 16:14:00 -> 2010-05-28 16:14:59).
     */
    static DateRange createMinutely(Date date)
    {
        Calendar start = new GregorianCalendar();
        start.setTime(date);

        // Set the time to the beginning of the hour of the requested date.
        start.set(start.get(Calendar.YEAR),
                  start.get(Calendar.MONTH),
                  start.get(Calendar.DATE),
                  start.get(Calendar.HOUR_OF_DAY),
                  start.get(Calendar.MINUTE),
                  start.getActualMinimum(Calendar.SECOND));
        start.set(Calendar.MILLISECOND, 0);

        Calendar end = new GregorianCalendar();
        end.setTime(date);

        // Set the day to the end of the day of the requested date.
        end.set(end.get(Calendar.YEAR),
                end.get(Calendar.MONTH),
                end.get(Calendar.DATE),
                end.get(Calendar.HOUR_OF_DAY),
                end.get(Calendar.MINUTE),
                end.getActualMaximum(Calendar.SECOND));
        end.set(Calendar.MILLISECOND, 999);

        return new DateRange(start.getTime(), end.getTime());
    }

    /**
     * Create a second range for a given time. So if the date is
     * "2010-05-28 16:14:08:594" then the returned range would be
     * (2010-05-28 16:14:08:000 -> 2010-05-28 16:14:08:000).
     */
    static DateRange createSecondly(Date date)
    {
        Calendar start = new GregorianCalendar();
        start.setTime(date);
        start.set(Calendar.MILLISECOND, 0);

        Calendar end = new GregorianCalendar();
        end.setTime(date);
        end.set(Calendar.MILLISECOND, 999);

        return new DateRange(start.getTime(), end.getTime());
    }

    public static DateRange create(Type type, Date date)
    {
        switch(type)
        {
            case SECONDLY:
                return createSecondly(date);
            case MINUTELY:
                return createMinutely(date);
            case HOURLY:
                return createHourly(date);
            case DAILY:
                return createDaily(date);
            case WEEKLY:
                return createWeekly(date);
            case MONTHLY:
                return createMonthly(date);
            case YEARLY:
                return createYearly(date);
            case FOREVER:
                return createForever(date);
            default:
                throw new IllegalArgumentException("Unsupported date range '" + type + "'.");
        }
    }

    public boolean includes(Date suspect)
    {
        return start.before(suspect) && end.after(suspect);
    }

    public boolean includesToday()
    {
        return includes(new Date());
    }

    @Override
    public String toString()
    {
        return start + " -> " + end;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof DateRange))
        {
            return false;
        }

        DateRange other = (DateRange)obj;
        return other.start.equals(start) && other.end.equals(end);
    }
}