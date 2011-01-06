package com.socrata.balboa.server.rest.filters;

import com.sun.jersey.spi.container.ContainerRequestFilter;
import com.sun.jersey.spi.container.ContainerRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LiteLoggingFilter implements ContainerRequestFilter
{
    private static Log log = LogFactory.getLog(LiteLoggingFilter.class);

    @Override
    public ContainerRequest filter(ContainerRequest request)
    {
        StringBuilder b = new StringBuilder();

        b.append("Server in-bound request: ").
                append(request.getMethod()).append(" ").
                append(request.getRequestUri().toASCIIString());

        log.info(b.toString());

        return request;
    }
}
