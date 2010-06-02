package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.Summary;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import me.prettyprint.cassandra.testutils.EmbeddedServerHelper;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CassandraDataStoreTest
{
    private static EmbeddedServerHelper embedded;
    
    @BeforeClass
    public static void setup() throws TTransportException, IOException, InterruptedException
    {
        embedded = new EmbeddedServerHelper();
        embedded.setup();
    }

    @AfterClass
    public static void teardown() throws IOException
    {
        embedded.teardown();
    }

    @Test
    public void testCreate() throws Exception
    {
        CassandraDataStore ds = get();

        Map<String, String> data = new HashMap<String, String>();
        data.put("test1", "1");
        data.put("test2", "2");
        Summary summary = new Summary(0, data);

        ds.persist("123", summary);
    }

    CassandraDataStore get()
    {
        return new CassandraDataStore(new String[] {"localhost:9170"}, "Metrics");
    }
}
