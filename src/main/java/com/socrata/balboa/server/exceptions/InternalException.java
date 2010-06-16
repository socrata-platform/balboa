package com.socrata.balboa.server.exceptions;

public class InternalException extends RuntimeException
{
    public InternalException()
    {
        super();
    }

    public InternalException(String msg)
    {
        super(msg);
    }

    public InternalException(String msg, Throwable cause)
    {
        super(msg, cause);
    }

    public InternalException(Throwable cause)
    {
        super("Internal error", cause);
    }
}
