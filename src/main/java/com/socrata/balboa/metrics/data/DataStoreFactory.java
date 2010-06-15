package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.data.impl.CassandraDataStore;
import com.socrata.balboa.metrics.data.impl.MapDataStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DataStoreFactory
{
    private static Log log = LogFactory.getLog(DataStoreFactory.class);

    public static DataStore get()
    {
        String environment = System.getProperty("socrata.env");

        if ("test".equals(environment))
        {
            log.debug("Retrieving a MapDataStore instance.");
            return MapDataStore.getInstance();
        }
        else
        {
            log.debug("Retrieving a CassandraDataStore instance.");
            return new CassandraDataStore(new String[] {"localhost:9160"}, "Metrics");
        }
    }
}
