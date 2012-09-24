package com.socrata.balboa.metrics.config;

import com.socrata.balboa.metrics.data.Period;

import java.io.IOException;
import java.util.Arrays;

public class PropertiesFixtureConfiguration extends Configuration
{
    PropertiesFixtureConfiguration() throws IOException
    {
        setSupportedTypes(Arrays.asList(
                Period.HOURLY, Period.DAILY, Period.MONTHLY
        ));

        setProperty("balboa.serializer", "protobuf");
        setProperty("balboa.datastore", "cassandra");
        setProperty("failfast.initialbackoff", "1000");
        setProperty("failfast.maxbackoff", "30000");
        setProperty("buffer.granularity", "120000");
    }
}
