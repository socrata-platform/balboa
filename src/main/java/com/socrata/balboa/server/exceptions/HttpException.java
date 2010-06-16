package com.socrata.balboa.server.exceptions;

public abstract class HttpException extends Exception
{
    public HttpException()
    {
        super();
    }

    public HttpException(String msg)
    {
        super(msg);
    }

    public HttpException(String msg, Throwable cause)
    {
        super(msg, cause);
    }

    public abstract int getStatus();
}
