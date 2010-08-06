package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.utils.MetricUtils;
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
    public void testRangeScan1() throws Exception
    {
        DataStore ds = get();

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("test1", 1);
        data.put("test2", 2);

        DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));
        Summary summary = new Summary(DateRange.Type.REALTIME, range.start.getTime(), data);
        ds.persist("testRangeScan1", summary);

        Iterator<Summary> iter = ds.find("testRangeScan1", range.start, range.end);

        Assert.assertTrue(iter.hasNext());

        Map<String, Object> results = MetricUtils.summarize(iter);

        Assert.assertEquals(1, results.get("test1"));
        Assert.assertEquals(2, results.get("test2"));

        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void testRangeScan2() throws Exception
    {
        DataStore ds = get();

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("test1", 1);
        data.put("test2", 2);

        DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));
        Summary summary = new Summary(DateRange.Type.REALTIME, range.start.getTime(), data);
        ds.persist("testRangeScan2", summary);

        data = new HashMap<String, Object>();
        data.put("test3", 3);
        summary = new Summary(DateRange.Type.REALTIME, range.end.getTime() + 1, data);
        ds.persist("testRangeScan2", summary);

        Iterator<Summary> iter = ds.find("testRangeScan2", range.start, range.end);

        Assert.assertTrue(iter.hasNext());

        Map<String, Object> results = MetricUtils.summarize(iter);

        Assert.assertEquals(1, results.get("test1"));
        Assert.assertEquals(2, results.get("test2"));
        
        Assert.assertFalse(results.containsKey("test3"));

        Assert.assertFalse(iter.hasNext());
    }
    
    @Test
    public void testRangeScan3() throws Exception
    {
        DataStore ds = get();

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("test1", 1);
        data.put("test2", 2);

        DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));
        Summary summary = new Summary(DateRange.Type.REALTIME, range.start.getTime(), data);
        ds.persist("testRangeScan3", summary);

        data = new HashMap<String, Object>();
        data.put("test3", 3);
        summary = new Summary(DateRange.Type.REALTIME, range.end.getTime() + 1, data);
        ds.persist("testRangeScan3", summary);

        DateRange.Type mostGranular = DateRange.Type.mostGranular(Configuration.get().getSupportedTypes());

        Iterator<Summary> iter = ds.find(
                "testRangeScan3",
                DateRange.create(mostGranular, range.start).start,
                DateRange.create(mostGranular, new Date(range.end.getTime() + 1)).end
        );

        Assert.assertTrue(iter.hasNext());

        Map<String, Object> results = MetricUtils.summarize(iter);

        Assert.assertEquals(1, results.get("test1"));
        Assert.assertEquals(2, results.get("test2"));
        Assert.assertEquals(3, results.get("test3"));

        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void testRangeScan4() throws Exception
    {
        DataStore ds = get();
        DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));

        Iterator<Summary> iter = ds.find("testRangeScan4", range.start, range.end);

        Assert.assertFalse(iter.hasNext());
    }
    
    @Test
    public void testCreate() throws Exception
    {
        DataStore ds = get();

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("test1", 1);
        data.put("test2", 2);

        DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));
        Summary summary = new Summary(DateRange.Type.REALTIME, range.start.getTime(), data);
        ds.persist("testCreate", summary);

        Iterator<Summary> iter = ds.find("testCreate", DateRange.Type.MONTHLY, new Date(0));

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

        DateRange range = DateRange.create(DateRange.Type.MONTHLY, new Date(0));
        Summary summary = new Summary(DateRange.Type.REALTIME, range.start.getTime(), data);
        ds.persist("testCreateSummaryActuallyUpdatesTheSummaryIfItAlreadyExists", summary);

        data = new HashMap<String, Object>();
        data.put("test1", 1);
        data.put("test2", 2);
        data.put("test3", 3);
        summary = new Summary(DateRange.Type.REALTIME, range.start.getTime(), data);
        ds.persist("testCreateSummaryActuallyUpdatesTheSummaryIfItAlreadyExists", summary);

        Iterator<Summary> iter = ds.find("testCreateSummaryActuallyUpdatesTheSummaryIfItAlreadyExists", DateRange.Type.MONTHLY, new Date(0));

        Assert.assertTrue(iter.hasNext());

        summary = iter.next();

        Assert.assertEquals(2, summary.getValues().get("test1"));
        Assert.assertEquals(4, summary.getValues().get("test2"));
        Assert.assertEquals(3, summary.getValues().get("test3"));

        Assert.assertFalse(iter.hasNext());
    }
}
