package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.config.PropertiesConfiguration;
import com.socrata.balboa.metrics.data.impl.*;

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
            throw new PropertiesConfiguration.ConfigurationException("Unable to determine which datastore to use because the configuration couldn't be read.", e);
        }

        if (datastore.equals("buffered-cassandra")) {
            return new BufferedDataStore(
                    new BadIdeasDataStore(
                            new Cassandra11DataStore(
                                    new Cassandra11QueryImpl(
                                            Cassandra11Util.context()))));
        }

        if (datastore.equals("cassandra"))
        {
            return new BadIdeasDataStore(
                    new Cassandra11DataStore(
                            new Cassandra11QueryImpl(
                                    Cassandra11Util.context())));
        }
        else
        {
            throw new PropertiesConfiguration.ConfigurationException("Unknown datastore '" + datastore + "'.");
        }
    }
}
