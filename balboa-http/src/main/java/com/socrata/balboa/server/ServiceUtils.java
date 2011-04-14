package com.socrata.balboa.server;

import com.socrata.balboa.server.exceptions.InvalidRequestException;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class ServiceUtils
{
    public static Date parseDate(String input) throws InvalidRequestException
    {
        DateFormat onlyDate = new SimpleDateFormat("yyyy-MM-dd");
        DateFormat dateWithTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

        onlyDate.setTimeZone(TimeZone.getTimeZone("UTC"));
        dateWithTime.setTimeZone(TimeZone.getTimeZone("UTC"));

        // First try to parse with the time component.
        try
        {
            return dateWithTime.parse(input);
        }
        catch (ParseException e)
        {
        }

        // Try to parse first without a time component.
        try
        {
            return onlyDate.parse(input);
        }
        catch (ParseException e)
        {
        }

        throw new InvalidRequestException("Unparsable date '" + input + "'.");
    }

    public static void validateRequired(Map<String, String> params, String[] required) throws InvalidRequestException
    {
        for (String param : required)
        {
            if (!params.containsKey(param))
            {
                throw new InvalidRequestException("Parameter '" + param + "' is required.");
            }
        }
    }

    public static Map<String, String> getParameters(HttpServletRequest request) throws IOException
    {
        Map params = request.getParameterMap();
        Map<String, String> results = new HashMap<String, String>();

        for (Object k : params.keySet())
        {
            String key = (String)k;
            String[] value = (String[])params.get(key);
            results.put(key, value[0]);
        }

        return results;
    }
}
