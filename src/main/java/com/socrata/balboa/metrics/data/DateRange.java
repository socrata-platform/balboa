package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.Summary.Type;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class DateRange
{
    public Date start;
    public Date end;

    public DateRange(Date start, Date end)
    {
        this.start = start;
        this.end = end;
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

    public static DateRange create(Type type, Date date)
    {
        switch(type)
        {
            case WEEKLY:
                return createWeekly(date);
            case MONTHLY:
                return createMonthly(date);
            case YEARLY:
                return createYearly(date);
            default:
                return createDaily(date);
        }
    }

    public boolean includesToday()
    {
        return start.before(new Date()) && end.after(new Date());
    }

    @Override
    public String toString()
    {
        return start + " -> " + end;
    }
}