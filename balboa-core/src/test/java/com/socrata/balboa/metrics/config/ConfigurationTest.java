package com.socrata.balboa.metrics.config;

import org.junit.Test;

public class ConfigurationTest
{
    @Test
    public void testInvalidEnvironmentNoCrash() throws Exception
    {
        try
        {
            Configuration.instance = null;
            System.setProperty("socrata.env", "snuffleupadata");
            Configuration.get();
        }
        finally
        {
            System.setProperty("socrata.env", "test");
        }
    }
}
