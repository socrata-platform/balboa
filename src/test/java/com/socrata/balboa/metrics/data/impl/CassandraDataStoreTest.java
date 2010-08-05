package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.data.*;
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

    @Test(expected=java.io.IOException.class)
    public void testLockingAKeyWontWriteIt() throws Exception
    {
        DataStore ds = get();

        Lock lock = LockFactory.get();
        lock.acquire("123");

        try
        {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("test1", 1);

            Summary summary = new Summary(DateRange.Type.REALTIME, new Date(0).getTime(), data);
            ds.persist("123", summary);
        }
        finally
        {
            lock.release("123");
        }
    }

    @Test
    public void testMakeSureThatManyWritesOnlySummarizeToOne() throws Exception
    {
        DataStore ds = get();

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("test1", 1);

        for (int i=0; i < 25; i++)
        {
            Summary summary = new Summary(DateRange.Type.REALTIME, new Date(0).getTime() + i, data);
            ds.persist("123", summary);
        }

        CassandraDataStore.QueryRobot iter = (CassandraDataStore.QueryRobot)ds.find("123", DateRange.Type.DAILY, new Date(-86400000), new Date());
        
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(1, iter.buffer.size());
    }
}
