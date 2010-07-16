package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.data.impl.CassandraDataStore;
import com.socrata.balboa.metrics.data.impl.MapDataStore;
import com.socrata.balboa.server.exceptions.InternalException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Arrays;

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
            try
            {
                Configuration config = Configuration.get();
                String serverConfig = config.getProperty("cassandra.servers");
                String keyspace = config.getProperty("cassandra.keyspace");

                if (serverConfig == null || keyspace == null)
                {
                    throw new InternalException("Either cassandra.servers or metrics.keyspace is not defined. Please check the configuration file and set the properties.");
                }

                // Servers in the configuration should be separated by a comma. Split it up for the connection.
                String[] servers = serverConfig.split(",");

                log.debug("Retrieving a CassandraDataStore instance '" + Arrays.toString(servers) + "' on keyspace '" + keyspace + "'.");
                return new CassandraDataStore(servers, keyspace);
            }
            catch (IOException e)
            {
                log.fatal("Unable to read the configuration file to figure out what to connect to.", e);
                throw new InternalException("Unable to read the configuration file to figure out what to connect to.", e);
            }
        }
    }
}
