package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.Summary;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public abstract class DataStoreTest
{
    public abstract DataStore get();
    
    @Test
    public void testCreate() throws Exception
    {
        DataStore ds = get();

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("test1", 1);
        data.put("test2", 2);

        DateRange range = DateRange.create(Summary.Type.MONTHLY, new Date(0));
        Summary summary = new Summary(Summary.Type.REALTIME, range.start.getTime(), data);
        ds.persist("testCreate", summary);

        Iterator<Summary> iter = ds.find("testCreate", Summary.Type.MONTHLY, new Date(0));

        Assert.assertTrue(iter.hasNext());

        summary = iter.next();

        Assert.assertEquals(1, summary.getValues().get("test1"));
        Assert.assertEquals(2, summary.getValues().get("test2"));

        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void testCreateSummaryActuallyUpdatesTheSummaryIfItAlreadyExists() throws Exception
    {
        DataStore ds = get();

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("test1", 1);
        data.put("test2", 2);

        DateRange range = DateRange.create(Summary.Type.MONTHLY, new Date(0));
        Summary summary = new Summary(Summary.Type.REALTIME, range.start.getTime(), data);
        ds.persist("testCreateSummaryActuallyUpdatesTheSummaryIfItAlreadyExists", summary);

        data = new HashMap<String, Object>();
        data.put("test1", 1);
        data.put("test2", 2);
        data.put("test3", 3);
        summary = new Summary(Summary.Type.REALTIME, range.start.getTime(), data);
        ds.persist("testCreateSummaryActuallyUpdatesTheSummaryIfItAlreadyExists", summary);

        Iterator<Summary> iter = ds.find("testCreateSummaryActuallyUpdatesTheSummaryIfItAlreadyExists", Summary.Type.MONTHLY, new Date(0));

        Assert.assertTrue(iter.hasNext());

        summary = iter.next();

        Assert.assertEquals(2, summary.getValues().get("test1"));
        Assert.assertEquals(4, summary.getValues().get("test2"));
        Assert.assertEquals(3, summary.getValues().get("test3"));

        Assert.assertFalse(iter.hasNext());
    }
}
