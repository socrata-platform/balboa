package com.socrata.balboa.metrics.config;

import org.junit.Test;

public class ConfigurationTest
{
    @Test
    public void testInvalidEnvironmentNoCrash() throws Exception
    {
        Configuration.instance = null;
        System.setProperty("socrata.env", "snuffleupadata");
        Configuration.get();
    }
}
