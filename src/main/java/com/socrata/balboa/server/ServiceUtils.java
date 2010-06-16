package com.socrata.balboa.server;

import com.socrata.balboa.server.exceptions.InvalidRequestException;
import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ServiceUtils
{
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
