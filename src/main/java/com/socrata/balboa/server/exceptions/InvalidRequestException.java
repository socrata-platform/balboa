package com.socrata.balboa.server.exceptions;

public class InvalidRequestException extends HttpException
{
    public InvalidRequestException()
    {
        super();
    }

    public InvalidRequestException(String msg)
    {
        super(msg);
    }

    @Override
    public int getStatus()
    {
        return 400;
    }
}
