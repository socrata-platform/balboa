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
        long l = new Date(0).getTime();
        Summary summary = new Summary(Type.MONTHLY, range.start.getTime(), data);
        ds.persist("123", summary);

        Iterator<Summary> iter = ds.find("123", Type.MONTHLY, new Date(0));

        Assert.assertTrue(iter.hasNext());

        summary = iter.next();

        Assert.assertFalse(iter.hasNext());
    }

    DataStore get()
    {
        return new CassandraDataStore(new String[] {"localhost:9170"}, "Metrics");
    }
}
