package com.socrata.balboa.metrics.config;

import com.socrata.balboa.metrics.data.DateRange;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public abstract class Configuration extends Properties
{
    public static Configuration instance;
    private List<DateRange.Period> supportedPeriods;

    public static synchronized Configuration get() throws IOException
    {
        String environment = System.getProperty("socrata.env");
        if (environment == null)
        {
            environment = "development";
        }

        environment = "test";

        if (instance == null)
        {
            if (environment.equals("test"))
            {
                instance = new PropertiesFixtureConfiguration();
            }
            else
            {
                instance = new PropertiesConfiguration();
            }
        }

        return instance;
    }

    /**
     * Don't use this, it's only for mocking.
     */
    synchronized public void setSupportedTypes(List<DateRange.Period> supportedPeriods)
    {
        this.supportedPeriods = supportedPeriods;
    }

    synchronized public List<DateRange.Period> getSupportedTypes()
    {
        if (supportedPeriods == null)
        {
            String[] types = ((String)getProperty("balboa.summaries")).split(",");

            supportedPeriods = new ArrayList<DateRange.Period>(types.length);
            for (String t : types)
            {
                supportedPeriods.add(DateRange.Period.valueOf(t.toUpperCase()));
            }
        }

        return supportedPeriods;
    }
}
