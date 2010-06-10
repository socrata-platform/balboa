package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.data.impl.MapDataStore;

public class DataStoreFactory
{
    public static DataStore get()
    {
        /*tring environment = System.getProperty("socrata.env");
        System.out.println(">>>>>>>>>>>>>>>> " + environment);

        if ("test".equals(environment))
        { */
            return MapDataStore.getInstance();
        /*}
        else
        {
            return new CassandraDataStore(new String[] {"localhost:9160"}, "Metrics");
        }*/
    }
}
