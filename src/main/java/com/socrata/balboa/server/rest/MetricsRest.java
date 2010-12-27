package com.socrata.balboa.server.rest;

import com.socrata.balboa.metrics.Metrics;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.impl.MessageProtos;
import com.socrata.balboa.metrics.impl.ProtocolBuffersMetrics;
import com.socrata.balboa.server.ServiceUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

@Path("/{entityId}")
public class MetricsRest
{
    @GET
    @Produces({"application/json", "application/x-protobuf"})
    public Response get(
            @PathParam("entityId") String entityId,
            @QueryParam("period") DateRange.Period period,
            @QueryParam("date") String date,
            @QueryParam("combine") String combine,
            @QueryParam("field") String field,
            @Context HttpHeaders headers
    ) throws IOException
    {
        DataStore ds = DataStoreFactory.get();
        DateRange range = DateRange.create(period, ServiceUtils.parseDate(date));

        Iterator<Metrics> iter = ds.find(entityId, period, range.start, range.end);
        Metrics metrics = Metrics.summarize(iter);
        metrics.setTimestamp(null);

        return render(headers.getMediaType(), metrics);
    }

    @GET
    @Path("range")
    @Produces({"application/json", "application/x-protobuf"})
    public Response range(
            @PathParam("entityId") String entityId,
            @QueryParam("start") String start,
            @QueryParam("end") String end,
            @Context HttpHeaders headers
    ) throws IOException
    {
        DataStore ds = DataStoreFactory.get();

        Date startDate = ServiceUtils.parseDate(start);
        Date endDate = ServiceUtils.parseDate(end);

        Iterator<Metrics> iter = ds.find(entityId, startDate, endDate);
        Metrics metrics = Metrics.summarize(iter);
        metrics.setTimestamp(null);

        return render(headers.getMediaType(), metrics);
    }

    @GET
    @Path("series")
    @Produces({"application/json", "application/x-protobuf"})
    public Response series(
            @PathParam("entityId") String entityId,
            @QueryParam("period") DateRange.Period period,
            @QueryParam("start") String start,
            @QueryParam("end") String end,
            @Context HttpHeaders headers
    ) throws IOException
    {
        DataStore ds = DataStoreFactory.get();

        Date startDate = ServiceUtils.parseDate(start);
        Date endDate = ServiceUtils.parseDate(end);

        Iterator<? extends Metrics> iter = ds.find(entityId, period, startDate, endDate);
        return render(headers.getMediaType(), iter);
    }

    private Response render(MediaType format, Iterator<? extends Metrics> metrics) throws IOException
    {
        if (format == MediaType.valueOf("application/x-protobuf"))
        {
            List<MessageProtos.PBMetrics> list = new ArrayList<MessageProtos.PBMetrics>();

            while (metrics.hasNext())
            {
                Metrics m = metrics.next();
                ProtocolBuffersMetrics pbm = new ProtocolBuffersMetrics(m);
                list.add(pbm.proto());
            }

            MessageProtos.PBMetricsSeries series = MessageProtos.PBMetricsSeries.newBuilder().addAllSeries(list).build();

            return Response.ok(series.toByteArray(), "application/x-protobuf").build();
        }
        else
        {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);

            return Response.ok(mapper.writeValueAsString(metrics), "application/json").build();
        }
    }

    private Response render(MediaType format, Metrics metrics) throws IOException
    {
        if (format == MediaType.valueOf("application/x-protobuf"))
        {
            ProtocolBuffersMetrics mapper = new ProtocolBuffersMetrics();
            mapper.merge(metrics);

            return Response.ok(mapper.serialize(), "application/x-protobuf").build();
        }
        else
        {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);

            return Response.ok(mapper.writeValueAsString(metrics), "application/json").build();
        }
    }
}
