package com.socrata.balboa.server.rest;

import com.socrata.balboa.metrics.Metrics;
import com.socrata.balboa.metrics.Timeslice;
import com.socrata.balboa.metrics.data.*;
import com.socrata.balboa.metrics.impl.ProtocolBuffersMetrics;
import com.socrata.balboa.server.ServiceUtils;
import com.socrata.balboa.server.exceptions.InvalidRequestException;
import com.yammer.metrics.core.TimerMetric;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

@Path("/metrics/{entityId}")
public class MetricsRest
{
    public static final TimerMetric seriesMeter = com.yammer.metrics.Metrics.newTimer(MetricsRest.class, "series queries", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    public static final TimerMetric rangeMeter = com.yammer.metrics.Metrics.newTimer(MetricsRest.class, "range queries", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    public static final TimerMetric periodMeter = com.yammer.metrics.Metrics.newTimer(MetricsRest.class, "period queries", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private static Log log = LogFactory.getLog(MetricsRest.class);

    @GET
    @Produces({"application/json", "application/x-protobuf"})
    public Response get(
            @PathParam("entityId") String entityId,
            @QueryParam("period") Period period,
            @QueryParam("date") String date,
            @QueryParam("combine") String combine,
            @QueryParam("field") String field,
            @Context HttpHeaders headers
    ) throws IOException
    {
        long begin = System.currentTimeMillis();

        try
        {
            DataStore ds = DataStoreFactory.get();
            DateRange range = DateRange.create(period, ServiceUtils.parseDate(date));

            Iterator<Metrics> iter = ds.find(entityId, period, range.start, range.end);
            Metrics metrics = Metrics.summarize(iter);

            if (combine != null)
            {
                metrics = metrics.combine(combine);
            }

            if (field != null)
            {
                metrics = metrics.filter(field);
            }

            return render(getMediaType(headers), metrics);
        }
        finally
        {
            periodMeter.update(System.currentTimeMillis() - begin, TimeUnit.MILLISECONDS);
        }
    }

    @GET
    @Path("range")
    @Produces({"application/json", "application/x-protobuf"})
    public Response range(
            @PathParam("entityId") String entityId,
            @QueryParam("start") String start,
            @QueryParam("end") String end,
            @QueryParam("combine") String combine,
            @QueryParam("field") String field,
            @Context HttpHeaders headers
    ) throws IOException
    {
        long begin = System.currentTimeMillis();

        try
        {
            DataStore ds = DataStoreFactory.get();

            Date startDate = ServiceUtils.parseDate(start);
            Date endDate = ServiceUtils.parseDate(end);

            Iterator<Metrics> iter = ds.find(entityId, startDate, endDate);
            Metrics metrics = Metrics.summarize(iter);

            if (combine != null)
            {
                metrics = metrics.combine(combine);
            }

            if (field != null)
            {
                metrics = metrics.filter(field);
            }

            return render(getMediaType(headers), metrics);
        }
        finally
        {
            rangeMeter.update(System.currentTimeMillis() - begin, TimeUnit.MILLISECONDS);
        }
    }

    @GET
    @Path("series")
    @Produces({"application/json", "application/x-protobuf"})
    public Response series(
            @PathParam("entityId") String entityId,
            @QueryParam("period") Period period,
            @QueryParam("start") String start,
            @QueryParam("end") String end,
            @Context HttpHeaders headers
    ) throws IOException
    {
        long begin = System.currentTimeMillis();

        try
        {
            DataStore ds = DataStoreFactory.get();

            Date startDate = ServiceUtils.parseDate(start);
            Date endDate = ServiceUtils.parseDate(end);

            Iterator<? extends Timeslice> iter = ds.slices(entityId, period, startDate, endDate);

            return render(getMediaType(headers), iter);
        }
        finally
        {
            seriesMeter.update(System.currentTimeMillis() - begin, TimeUnit.MILLISECONDS);
        }
    }

    @GET
    @Path("meta")
    @Produces("application/json")
    public Response meta(
            @PathParam("entityId") String entityId,
            @Context HttpHeaders headers
    ) throws IOException
    {
        throw new UnsupportedOperationException("Getting metadata is no longer implemented. Contact your System Administrator");
    }

    MediaType getMediaType(HttpHeaders headers)
    {
        MediaType format = headers.getMediaType();
        if (format == null)
        {
            for (MediaType type : headers.getAcceptableMediaTypes())
            {
                format = type;
                break;
            }
        }

        return format;
    }

    private Response render(MediaType format, Iterator<? extends Timeslice> metrics) throws IOException
    {
        if (format.getSubtype().equals("x-protobuf"))
        {
            throw new InvalidRequestException("Unable to serialize timeslices to protobuf");
        }
        else
        {
            return renderJson(metrics);
        }
    }

    private Response render(MediaType format, Metrics metrics) throws IOException
    {
        if (format.getSubtype().equals("x-protobuf"))
        {
            ProtocolBuffersMetrics mapper = new ProtocolBuffersMetrics();
            mapper.merge(metrics);

            return Response.ok(mapper.serialize(), "application/x-protobuf").build();
        }
        else
        {
            return renderJson(metrics);
        }
    }

    private Response renderJson(Object object) throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
        mapper.getSerializationConfig().setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
        mapper.configure(SerializationConfig.Feature.WRITE_ENUMS_USING_TO_STRING, true);

        return Response.ok(mapper.writeValueAsString(object), "application/json").build();
    }
}
