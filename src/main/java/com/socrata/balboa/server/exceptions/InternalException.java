package com.socrata.balboa.server.exceptions;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

public class InternalException extends WebApplicationException
{
    public InternalException()
    {
        super(Response.status(500).build());
    }

    public InternalException(String msg)
    {
        super();
    }

    public InternalException(String msg, Throwable cause)
    {
        super(cause);
    }

    public InternalException(Throwable cause)
    {
        super(cause);
    }
}
