package com.socrata.balboa.metrics.data;

import java.util.*;

/**
 * Holds a DateRange; but contains methods for finding the Dates for a given period.
 */
public class DateRange {

    public Date start;
    public Date end;

    public DateRange(Date start, Date end) {
        if (start.after(end)) {
            throw new IllegalArgumentException("The start time must be before the end time '" + start + "' !< '" + end + "'.");
        }

        this.start = start;
        this.end = end;
    }

    public static boolean liesOnBoundary(Date date, Period typeOfBoundary) {
        DateRange range = DateRange.create(typeOfBoundary, date);

        return range.start.equals(date) || range.end.equals(date);
    }

    /**
     * Get a date range that covers everything in the past/present/future. Kind
     * of like Timecop.
     */
    static DateRange createForever(Date date) {
        Calendar start = new GregorianCalendar();
        start.setTimeZone(TimeZone.getTimeZone("UTC"));

        // Trim the day of the year off the requested date.
        start.set(start.getActualMinimum(Calendar.YEAR),
                start.getActualMinimum(Calendar.MONTH),
                start.getActualMinimum(Calendar.DATE),
                start.getActualMinimum(Calendar.HOUR_OF_DAY),
                start.getActualMinimum(Calendar.MINUTE),
                start.getActualMinimum(Calendar.SECOND));
        start.set(Calendar.MILLISECOND, 0);

        Calendar end = new GregorianCalendar();
        end.setTimeZone(TimeZone.getTimeZone("UTC"));
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
    static DateRange createYearly(Date date) {
        Calendar start = new GregorianCalendar();
        start.setTimeZone(TimeZone.getTimeZone("UTC"));
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
        end.setTimeZone(TimeZone.getTimeZone("UTC"));
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
    static DateRange createMonthly(Date date) {
        Calendar start = new GregorianCalendar();
        start.setTimeZone(TimeZone.getTimeZone("UTC"));
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
        end.setTimeZone(TimeZone.getTimeZone("UTC"));
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
    static DateRange createWeekly(Date date) {
        Calendar start = new GregorianCalendar();
        start.setTimeZone(TimeZone.getTimeZone("UTC"));
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
        end.setTimeZone(TimeZone.getTimeZone("UTC"));
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
    static DateRange createDaily(Date date) {
        Calendar start = new GregorianCalendar();
        start.setTimeZone(TimeZone.getTimeZone("UTC"));
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
        end.setTimeZone(TimeZone.getTimeZone("UTC"));
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
    static DateRange createHourly(Date date) {
        Calendar start = new GregorianCalendar();
        start.setTimeZone(TimeZone.getTimeZone("UTC"));
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
        end.setTimeZone(TimeZone.getTimeZone("UTC"));
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
    static DateRange createFifteenMinutely(Date date) {
        Calendar start = new GregorianCalendar();
        start.setTimeZone(TimeZone.getTimeZone("UTC"));
        start.setTime(date);

        // Set the time to the beginning of the hour of the requested date.
        start.set(start.get(Calendar.YEAR),
                start.get(Calendar.MONTH),
                start.get(Calendar.DATE),
                start.get(Calendar.HOUR_OF_DAY),
                (start.get(Calendar.MINUTE) / 15) * 15,
                start.getActualMinimum(Calendar.SECOND));
        start.set(Calendar.MILLISECOND, 0);

        Calendar end = new GregorianCalendar();
        end.setTimeZone(TimeZone.getTimeZone("UTC"));
        end.setTime(date);

        // Set the day to the end of the day of the requested date.
        end.set(end.get(Calendar.YEAR),
                end.get(Calendar.MONTH),
                end.get(Calendar.DATE),
                end.get(Calendar.HOUR_OF_DAY),
                (start.get(Calendar.MINUTE) / 15 + 1) * 15 - 1,
                end.getActualMaximum(Calendar.SECOND));
        end.set(Calendar.MILLISECOND, 999);

        return new DateRange(start.getTime(), end.getTime());
    }

    /**
     * Create a minute range for a given time. So if the date is
     * "2010-05-28 16:14:08" then the returned range would be
     * (2010-05-28 16:14:00 -> 2010-05-28 16:14:59).
     */
    static DateRange createMinutely(Date date) {
        Calendar start = new GregorianCalendar();
        start.setTimeZone(TimeZone.getTimeZone("UTC"));
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
        end.setTimeZone(TimeZone.getTimeZone("UTC"));
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
     * (2010-05-28 16:14:08:000 -> 2010-05-28 16:14:08:999).
     */
    static DateRange createSecondly(Date date) {
        Calendar start = new GregorianCalendar();
        start.setTimeZone(TimeZone.getTimeZone("UTC"));
        start.setTime(date);
        start.set(Calendar.MILLISECOND, 0);

        Calendar end = new GregorianCalendar();
        end.setTimeZone(TimeZone.getTimeZone("UTC"));
        end.setTime(date);
        end.set(Calendar.MILLISECOND, 999);

        return new DateRange(start.getTime(), end.getTime());
    }

    public static DateRange create(Period period, Date date) {
        switch (period) {
            case SECONDLY:
                return createSecondly(date);
            case FIFTEEN_MINUTE:
                return createFifteenMinutely(date);
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
                throw new IllegalArgumentException("Unsupported date range '" + period + "'.");
        }
    }

    // Return a list of dates aligned to the given period for this date range
    public List<Date> toDates(Period period) {
        Date curr = start;
        List<Date> dates = new LinkedList<Date>();
        while (curr.before(end)) {
            DateRange range = DateRange.create(period, curr); // align date to boundary
            dates.add(range.start);
            curr = new Date(range.end.getTime() + 1);
        }
        return dates;
    }

    public boolean includes(Date suspect) {
        return (start.equals(suspect) || start.before(suspect)) &&
                (end.equals(suspect) || end.after(suspect));
    }

    public boolean includesToday() {
        return includes(new Date());
    }

    @Override
    public String toString() {
        return start + " -> " + end;
    }

    @Override
    public int hashCode() {
        return start.hashCode() + end.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DateRange)) {
            return false;
        }

        DateRange other = (DateRange) obj;
        return other.start.equals(start) && other.end.equals(end);
    }
}