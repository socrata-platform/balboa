package com.socrata.balboa.server.rest;

import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

@Path("/entities")
public class EntitiesRest
{
    @GET
    @Produces("application/json")
    public Response index(
            @QueryParam("filter") String filter,
            @DefaultValue("-1") @QueryParam("limit") int limit
    ) throws IOException
    {
        throw new UnsupportedOperationException("Getting the list of entities over http is a really bad idea. Contact your System Administrator");
    }

}
