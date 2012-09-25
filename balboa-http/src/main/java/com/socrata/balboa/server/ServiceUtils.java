package com.socrata.balboa.server;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class ServiceUtils
{
    public static scala.Option<Date> parseDate(String input)
    {
        DateFormat onlyDate = new SimpleDateFormat("yyyy-MM-dd");
        DateFormat dateWithTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

        onlyDate.setTimeZone(TimeZone.getTimeZone("UTC"));
        dateWithTime.setTimeZone(TimeZone.getTimeZone("UTC"));

        // First try to parse with the time component.
        try
        {
            return scala.Option.apply(dateWithTime.parse(input));
        }
        catch (ParseException e)
        {
        }

        // Try to parse first without a time component.
        try
        {
            return scala.Option.apply(onlyDate.parse(input));
        }
        catch (ParseException e)
        {
        }

        return scala.Option.apply((Date) null);
    }
}
