package com.socrata.balboa.server;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.measurements.combining.Combinator;
import com.socrata.balboa.metrics.messaging.Receiver;
import com.socrata.balboa.metrics.messaging.ReceiverFactory;
import com.socrata.balboa.server.exceptions.HttpException;
import com.socrata.balboa.server.exceptions.InvalidRequestException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.pojava.datetime.DateTime;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

public class BalboaServlet extends HttpServlet
{
    private static Log log = LogFactory.getLog(BalboaServlet.class);
    private Receiver receiver;
    
    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        // We're always JSON, no matter what.
        response.setContentType("application/json; charset=utf-8");

        try
        {
            String[] path = request.getPathInfo().split("/");
            String entityId = path[1];

            if (!"GET".equals(request.getMethod()))
            {
                throw new InvalidRequestException("Unsupported method '" + request.getMethod() + "'.");
            }

            Object result = fulfillGet(entityId, request, response);

            // Write the response out.
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
            mapper.writeValue(response.getOutputStream(), result);
        }
        catch (HttpException e)
        {
            // Write out any "expected" errors.
            log.debug("Unable to fullfil request because there was an HTTP error.", e);
            response.setStatus(e.getStatus());
            response.getOutputStream().write(e.getMessage().getBytes());
        }
        catch (Throwable e)
        {
            // Any other problems were things we weren't expecting.
            log.fatal("Unexpected exception handling a request.", e);
            response.setStatus(500);
            response.getOutputStream().write("Internal error.".getBytes());
        }
    }

    @Override
    public void init() throws ServletException
    {
        super.init();

        Logger.getLogger("com.socrata.balboa").setLevel(Level.DEBUG);

        // Initialize our receiver and it will automatically connect.
        receiver = ReceiverFactory.get();
    }

    Object fulfillGet(String id, HttpServletRequest request, HttpServletResponse response) throws IOException, InvalidRequestException
    {
        MetricsService service = new MetricsService();
        Map<String, String> params = ServiceUtils.getParameters(request);
        
        ServiceUtils.validateRequired(params, new String[] {"type", "date"});

        Summary.Type type = Summary.Type.valueOf(params.get("type"));
        DateTime date = DateTime.parse(params.get("date"));

        if (date == null)
        {
            throw new InvalidRequestException("Unrecognized date format '" + params.get("date") + "'.");
        }

        DateRange range = DateRange.create(type, date.toDate());

        if (params.containsKey("field"))
        {
            return service.get(id, type, (String)params.get("field"), getCombinator(params), range);
        }
        else
        {
            return service.get(id, type, range);
        }
    }

    Combinator getCombinator(Map<String, String> params) throws InvalidRequestException
    {
        ServiceUtils.validateRequired(params, new String[] {"combinator"});
        String combinatorClass = "com.socrata.balboa.metrics.measurements.combining." + params.get("combinator");

        try
        {
            Class klass = Class.forName(combinatorClass);
            return (Combinator)klass.getConstructor().newInstance();
        }
        catch (Exception e)
        {
            throw new InvalidRequestException("Invalid combinator '" + combinatorClass + "'.");
        }
    }
}
