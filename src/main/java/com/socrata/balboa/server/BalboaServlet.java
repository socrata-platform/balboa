package com.socrata.balboa.server;

import com.socrata.balboa.exceptions.InternalException;
import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.measurements.combining.Combinator;
import com.socrata.balboa.metrics.measurements.combining.Sum;
import com.socrata.balboa.metrics.measurements.preprocessing.JsonPreprocessor;
import com.socrata.balboa.metrics.measurements.preprocessing.Preprocessor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pojava.datetime.DateTime;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

public class BalboaServlet extends HttpServlet
{
    private static Log log = LogFactory.getLog(BalboaServlet.class);
    
    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        // We're always JSON, no matter what.
        response.setContentType("application/json; charset=utf-8");

        try
        {
            String[] path = request.getPathInfo().split("/");
            String entityId = path[1];

            if ("GET".equals(request.getMethod()))
            {
                fulfillGet(entityId, request, response);
            }
            else
            {
                throw new Exception("Unsupported method '" + request.getMethod() + "'.");
            }
        }
        catch (Throwable e)
        {
            log.fatal("Unexpected exception handling a request.", e);
            response.setStatus(500);
        }
    }

    void fulfillGet(String id, HttpServletRequest request, HttpServletResponse response)
    {
        Map params = request.getParameterMap();

        String field = ((String[])params.get("field"))[0];

        DateRange range = null;
        if (params.containsKey("type") && params.containsKey("date"))
        {
            String type = ((String[])params.get("type"))[0];
            Date date = DateTime.parse(((String[])params.get("date"))[0]).toDate();
            range = DateRange.create(Summary.Type.valueOf(type), date);
        }
        else if (params.containsKey("type"))
        {
            // If no date is provided, then just use today along with the type.
            String type = ((String[])params.get("type"))[0];
            range = DateRange.create(Summary.Type.valueOf(type), new Date());
        }
        else
        {
            throw new InternalException("Invalid request. Parameter 'type' is required.");
        }

        // TODO: Support other preprocessors.
        Preprocessor preprocessor = new JsonPreprocessor();

        // TODO: Support other combinators.
        Combinator combinator = new Sum();

        try
        {
            // Query for the metric.
            /*Object metrics = MetricReader.read(id, field, range.start, range.end, preprocessor, combinator);

            // And now write result to the response.
            ObjectMapper mapper = new ObjectMapper();
            mapper.writeValue(response.getOutputStream(), metrics);*/
        }
        catch (Exception e)
        {
            throw new InternalException("There was an error processing the request.", e);
        }
    }
}
