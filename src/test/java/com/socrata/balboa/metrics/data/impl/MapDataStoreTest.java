package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreTest;

public class MapDataStoreTest extends DataStoreTest
{
    @Override
    public DataStore get()
    {
        return new MapDataStore();
    }
}
