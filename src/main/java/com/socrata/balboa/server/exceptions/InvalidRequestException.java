package com.socrata.balboa.server.exceptions;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

public class InvalidRequestException extends WebApplicationException
{
    public InvalidRequestException()
    {
        super(Response.status(400).build());
    }

    public InvalidRequestException(String msg)
    {
        super(Response.status(400).entity(msg).build());
    }
}
