package com.socrata.balboa.server.rest.exceptions;

import com.sun.jersey.api.NotFoundException;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class NotFoundExceptionRest implements ExceptionMapper<NotFoundException>
{
    @Override
    public Response toResponse(NotFoundException e)
    {
        return Response.status(404).
                entity("{\"error\": 404, \"message\": \"Not found.\"}").
                build();
    }
}
