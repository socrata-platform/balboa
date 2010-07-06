package com.socrata.balboa.metrics.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Cascading properties. The default "config.properties" is always loaded first
 * and then a second "config.<environment>.properties" file is loaded
 * (potentially overwriting values in the first) if available.
 */
public class Configuration extends Properties
{
    private static Log log = LogFactory.getLog(Configuration.class);

    static Configuration instance;

    public static synchronized Configuration get() throws IOException
    {
        if (instance == null)
        {
            return new Configuration();
        }
        else
        {
            return instance;
        }
    }

    public Configuration() throws IOException
    {
        String environment = System.getProperty("socrata.env");

        // First load the base configuration file (config.properties) which is required in all environments.
        load(Configuration.class.getClassLoader().getResourceAsStream("config/config.properties"));

        // Now load the environment specific configuration file (if it exists) which is not required, so don't error if
        // it fails.
        try
        {
            if (environment != null)
            {
                InputStream stream = Configuration.class.getClassLoader().getResourceAsStream("config/config." + environment + ".properties");
                if (stream != null)
                {
                    load(stream);
                }
            }
        }
        catch (IOException e)
        {
            log.warn("Unable to load environment specific configuration file for '" + environment + "', but it's not " +
                    "required so I'll just ignore this error.");
        }
    }
}
