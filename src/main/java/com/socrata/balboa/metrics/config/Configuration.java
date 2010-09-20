package com.socrata.balboa.metrics.config;

import com.socrata.balboa.metrics.data.DateRange;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
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

    private List<DateRange.Type> supportedTypes;

    public static synchronized Configuration get() throws IOException
    {
        if (instance == null)
        {
            instance = new Configuration();
        }
        
        return instance;
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

    /**
     * Don't use this, it's only for mocking.
     */
    synchronized public void setSupportedTypes(List<DateRange.Type> supportedTypes)
    {
        this.supportedTypes = supportedTypes;
    }

    synchronized public List<DateRange.Type> getSupportedTypes()
    {
        if (supportedTypes == null)
        {
            String[] types = ((String)getProperty("balboa.summaries")).split(",");

            supportedTypes = new ArrayList<DateRange.Type>(types.length);
            for (String t : types)
            {
                supportedTypes.add(DateRange.Type.valueOf(t.toUpperCase()));
            }
        }

        return supportedTypes;
    }
}
