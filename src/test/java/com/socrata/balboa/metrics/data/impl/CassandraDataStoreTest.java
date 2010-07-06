package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.Summary.Type;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreTest;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class CassandraDataStoreTest extends DataStoreTest
{
    private static CassandraHelper embedded;

    public DataStore get()
    {
        return new CassandraDataStore(new String[] {"localhost:9170"}, "Metrics");
    }
    
    @BeforeClass
    public static void setup() throws TTransportException, IOException, InterruptedException
    {
        embedded = new CassandraHelper();
        embedded.setup();
    }

    @AfterClass
    public static void teardown() throws IOException
    {
        embedded.teardown();
    }

    @Test
    public void testIteratorOverABufferLargerThanTheDefault() throws Exception
    {
        DataStore ds = get();

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("test1", 1);

        for (int i=0; i < CassandraDataStore.QueryRobot.QUERYBUFFER * 2; i++)
        {
            Summary summary = new Summary(Type.REALTIME, new Date(0).getTime() + i, data);
            ds.persist("123", summary);
        }

        CassandraDataStore.QueryRobot iter = (CassandraDataStore.QueryRobot)ds.find("123", Type.REALTIME, new Date(0), new Date());

        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(CassandraDataStore.QueryRobot.QUERYBUFFER, iter.buffer.size());

        while (iter.buffer.size() > 0)
        {
            iter.next();
        }

        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(CassandraDataStore.QueryRobot.QUERYBUFFER, iter.buffer.size());
    }
}
