package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.Summary.Type;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DateRange;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CassandraDataStoreTest
{
    private static CassandraHelper embedded;
    
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
    public void testCreate() throws Exception
    {
        DataStore ds = get();

        Map<String, String> data = new HashMap<String, String>();
        data.put("test1", "1");
        data.put("test2", "2");

        DateRange range = DateRange.create(Type.MONTHLY, new Date(0));
        Summary summary = new Summary(Type.MONTHLY, range.start.getTime(), data);
        ds.persist("123", summary);

        Iterator<Summary> iter = ds.find("123", Type.MONTHLY, new Date(0));

        Assert.assertTrue(iter.hasNext());

        summary = iter.next();

        Assert.assertEquals("1", summary.getValues().get("test1"));
        Assert.assertEquals("2", summary.getValues().get("test2"));

        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void testCreateSummaryActuallyUpdatesTheSummaryIfItAlreadyExists() throws Exception
    {
        DataStore ds = get();

        Map<String, String> data = new HashMap<String, String>();
        data.put("test1", "1");
        data.put("test2", "2");

        DateRange range = DateRange.create(Type.MONTHLY, new Date(0));
        Summary summary = new Summary(Type.MONTHLY, range.start.getTime(), data);
        ds.persist("123", summary);

        data.put("test3", "3");
        ds.persist("123", summary);

        Iterator<Summary> iter = ds.find("123", Type.MONTHLY, new Date(0));

        Assert.assertTrue(iter.hasNext());

        summary = iter.next();

        Assert.assertEquals("1", summary.getValues().get("test1"));
        Assert.assertEquals("2", summary.getValues().get("test2"));
        Assert.assertEquals("3", summary.getValues().get("test3"));

        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void testIteratorOverABufferLargerThanTheDefault() throws Exception
    {
        DataStore ds = get();

        Map<String, String> data = new HashMap<String, String>();
        data.put("test1", "1");

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

    DataStore get()
    {
        return new CassandraDataStore(new String[] {"localhost:9170"}, "Metrics");
    }
}
