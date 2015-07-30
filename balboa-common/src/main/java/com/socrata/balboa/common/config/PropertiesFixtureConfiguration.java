package com.socrata.balboa.common.config;

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
        setProperty("cassandra.servers", "localhost:9160");
        setProperty("cassandra.keyspace", "Metrics3");
        setProperty("cassandra.maxpoolsize", "1");
        setProperty("cassandra.sotimeout", "1000");
        setProperty("balboa.serializer", "protobuf");
        setProperty("balboa.datastore", "cassandra");
        setProperty("failfast.initialbackoff", "1000");
        setProperty("failfast.maxbackoff", "30000");
        setProperty("buffer.granularity", "120000");
    }
}
