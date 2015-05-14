package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.config.ConfigurationException;
import com.socrata.balboa.metrics.data.impl.*;

import java.io.IOException;


public class DataStoreFactory
{

    private volatile static DataStore defaultDataStore = null;
    public static DataStore get() {
        if (defaultDataStore != null)
            return defaultDataStore;
        try {
            synchronized(DataStoreFactory.class) {
                if (defaultDataStore != null)
                    return defaultDataStore;
                defaultDataStore = get(Configuration.get());
            }
            return defaultDataStore;
        } catch (IOException e) {
            throw new ConfigurationException("Unable to determine which datastore to use because the configuration couldn't be read.", e);
        }
    }

    public static DataStore get(Configuration conf)
    {
        String datastore = (String)conf.get("balboa.datastore");

        if (datastore.equals("buffered-cassandra")) {
            System.out.println("Using buffered cassandra");
            return new BufferedDataStore(
                    new BadIdeasDataStore(
                            new Cassandra11DataStore(
                                    new Cassandra11QueryImpl(
                                            Cassandra11Util.initializeContext(conf)))));
        }

        if (datastore.equals("cassandra"))
        {
            System.out.println("Using non buffered cassandra");
            return new BadIdeasDataStore(
                    new Cassandra11DataStore(
                            new Cassandra11QueryImpl(
                                    Cassandra11Util.initializeContext(conf))));
        }
        else
        {
            throw new ConfigurationException("Unknown datastore '" + datastore + "'.");
        }
    }
}
