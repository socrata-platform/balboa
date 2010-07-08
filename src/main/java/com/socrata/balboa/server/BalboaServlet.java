package com.socrata.balboa.server;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.messaging.Receiver;
import com.socrata.balboa.metrics.messaging.ReceiverFactory;
import com.socrata.balboa.server.exceptions.HttpException;
import com.socrata.balboa.server.exceptions.InvalidRequestException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.pojava.datetime.DateTime;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

public class BalboaServlet extends HttpServlet
{
    private static Log log = LogFactory.getLog(BalboaServlet.class);

    /**
     * Not used, but assigned so that the receiver doesn't get garbage
     * collected.
     */
    Receiver receiver;
    
    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        // We're always JSON, no matter what.
        response.setContentType("application/json; charset=utf-8");

        try
        {
            String[] path = request.getPathInfo().split("/");

            if (path.length < 2)
            {
                throw new InvalidRequestException("Unknown resource '" + request.getPathInfo() + "'.");
            }
            else if (!"GET".equals(request.getMethod()))
            {
                throw new InvalidRequestException("Unsupported method '" + request.getMethod() + "'.");
            }

            String entityId = path[1];
            Object result = fulfillGet(entityId, ServiceUtils.getParameters(request));

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
    public void init(ServletConfig config) throws ServletException
    {
        super.init(config);

        try
        {
            // Initialize the configuration so that we set any log4j or external
            // configuration values properly.
            Configuration.get();
        }
        catch (IOException e)
        {
            throw new ServletException("Unable to load the configuration.", e);
        }

        // Initialize our receiver and it will automatically connect.
        receiver = ReceiverFactory.get();
    }

    Object single(String id, Map<String, String> params) throws InvalidRequestException, IOException
    {
        MetricsService service = new MetricsService();
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
            return service.get(id, type, (String)params.get("field"), range);
        }
        else
        {
            return service.get(id, type, range);
        }
    }

    Object series(String id, Map<String, String> params) throws InvalidRequestException, IOException
    {
        MetricsService service = new MetricsService();
        ServiceUtils.validateRequired(params, new String[] {"series", "start", "end"});

        Summary.Type type = Summary.Type.valueOf(params.get("series"));

        DateTime nominalStart = DateTime.parse(params.get("start"));
        DateTime nominalEnd = DateTime.parse(params.get("end"));

        DateRange range = new DateRange(
                DateRange.create(type, nominalStart.toDate()).start,
                DateRange.create(type, nominalEnd.toDate()).end
        );

        if (params.containsKey("field"))
        {
            return service.series(id, type, (String)params.get("field"), range);
        }
        else
        {
            return service.series(id, type, range);
        }
    }

    Object fulfillGet(String id, Map<String, String> params) throws IOException, InvalidRequestException
    {
        if (params.containsKey("series"))
        {
            return series(id, params);
        }
        else
        {
            return single(id, params);
        }
    }
}
