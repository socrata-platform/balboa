package com.socrata.balboa.metrics.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Cascading properties. The default "config.properties" is always loaded first
 * and then a second "config.<environment>.properties" file is loaded
 * (potentially overwriting values in the first) if available.
 */
public class PropertiesConfiguration extends Configuration
{
    private static final Logger logger = LoggerFactory.getLogger(PropertiesConfiguration.class);

    public PropertiesConfiguration() throws IOException
    {
        String override = System.getProperty("balboa.config");

        // First load the base configuration file (config.properties) which is required in all environments.
        load(PropertiesConfiguration.class.getClassLoader().getResourceAsStream("config/config.properties"));

        if (override != null)
        {
            File config = new File(override);
            load(config);
        }
        else
        {
            try
            {
                load(new File("/etc/balboa.properties"));
            }
            catch (IOException e)
            {
                System.err.println("An override wasn't provided and /etc/balboa.properties doesn't " +
                                 "exist or can't be loaded. Using the default " +
                                 "configuration values. Unless you're a " +
                                 "developer this probably isn't what you want.");
            }
        }
    }

    void load(File config) throws IOException
    {
        if (config.exists())
        {
            InputStream stream = new FileInputStream(config);
            load(stream);
        }
        else
        {
            System.err.println("A configuration override was provided but the file (" + config + ") doesn't exist.");
            throw new IOException("A configuration override was provided but the file (" + config + ") doesn't exist.");
        }
    }
}
