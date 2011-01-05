package com.socrata.balboa.server.rest;

import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.apache.log4j.PropertyConfigurator;

import javax.servlet.ServletException;
import java.io.IOException;
import java.util.Properties;

public class RestServlet extends ServletContainer
{
    @Override
    public void init() throws ServletException
    {
        super.init();

        try
        {
            Properties p = new Properties();
            p.load(RestServlet.class.getClassLoader().getResourceAsStream("config/config.properties"));
            PropertyConfigurator.configure(p);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to load configuration.", e);
        }
    }
}
