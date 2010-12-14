package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.data.impl.CassandraDataStore;
import com.socrata.balboa.server.exceptions.InternalException;

import java.io.IOException;


public class DataStoreFactory
{
    public static DataStore get()
    {
        String datastore;
        
        try
        {
            datastore = (String)Configuration.get().get("balboa.datastore");
        }
        catch (IOException e)
        {
            throw new InternalException("Unable to determine which datastore to use because the configuration couldn't be read.", e);
        }

        if (datastore.equals("cassandra"))
        {
            return new CassandraDataStore();
        }
        else
        {
            throw new InternalException("Unknown datastore '" + datastore + "'.");
        }
    }
}
