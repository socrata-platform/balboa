package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.data.impl.CassandraDataStore;

public class DataStoreFactory
{
    public static DataStore get()
    {
        // TODO: Move this to a configuration file.
        return new CassandraDataStore(new String[] {"localhost:9160"}, "Metrics");
    }
}
