package com.socrata.balboa.server.rest;

import com.socrata.balboa.metrics.config.Configuration;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.apache.log4j.PropertyConfigurator;

import javax.servlet.ServletException;
import java.io.IOException;

public class RestServlet extends ServletContainer
{
    @Override
    public void init() throws ServletException
    {
        super.init();

        try
        {
            Configuration config = Configuration.get();
            PropertyConfigurator.configure(config);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to load configuration.", e);
        }
    }
}
