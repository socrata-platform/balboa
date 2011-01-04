package com.socrata.balboa.metrics.config;

import com.socrata.balboa.metrics.data.DateRange;

import java.io.IOException;
import java.util.Arrays;

public class PropertiesFixtureConfiguration extends Configuration
{
    PropertiesFixtureConfiguration() throws IOException
    {
        setSupportedTypes(Arrays.asList(
                DateRange.Period.HOURLY, DateRange.Period.DAILY, DateRange.Period.MONTHLY
        ));

        setProperty("balboa.serializer", "protobuf");
        setProperty("balboa.datastore", "cassandra");
    }
}
