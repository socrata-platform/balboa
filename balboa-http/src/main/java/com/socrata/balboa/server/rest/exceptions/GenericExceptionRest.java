package com.socrata.balboa.server.rest.exceptions;

import org.codehaus.jackson.map.ObjectMapper;

import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Provider
public class GenericExceptionRest implements ExceptionMapper<Exception>
{
    @Override
    @Produces("application/json")
    public Response toResponse(Exception throwable)
    {
        Map<String, Object> error = new HashMap<String, Object>();
        error.put("error", 500);
        error.put("message", throwable.getMessage());

        ObjectMapper mapper = new ObjectMapper();

        try
        {
            return Response.status(500).
                    entity(mapper.writeValueAsString(error)).
                    type("application/json").build();
        }
        catch (IOException e)
        {
            return Response.status(500).
                    entity("{\"error\": 500, \"message\": \"Unserializable error.\"}").
                    build();
        }
    }
}
