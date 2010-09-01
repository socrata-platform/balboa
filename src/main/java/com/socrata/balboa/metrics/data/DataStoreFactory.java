package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.data.impl.CassandraDataStore;

public class DataStoreFactory
{
    public static DataStore get()
    {
        return new CassandraDataStore();
    }
}
