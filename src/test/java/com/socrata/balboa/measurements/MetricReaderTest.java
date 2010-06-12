package com.socrata.balboa.measurements;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.data.impl.MapDataStore;
import com.socrata.balboa.metrics.measurements.MetricReader;
import com.socrata.balboa.metrics.measurements.combining.Sum;
import com.socrata.balboa.metrics.measurements.preprocessing.JsonPreprocessor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class MetricReaderTest
{
    @After
    public void teardown()
    {
        MapDataStore.destroy();
    }

    @Test
    public void testSummarizeReallyBigNumbers() throws Exception
    {
        DataStore ds = DataStoreFactory.get();
        
        Map<String, String> data = new HashMap<String, String>();
        data.put("views", Long.toString(Long.MAX_VALUE));
        data.put("hits", "123");

        // Create a new date range that includes today.
        DateRange range = DateRange.create(Summary.Type.MONTHLY, new Date());

        // Create a summary that's within that date range
        Summary summary = new Summary(Summary.Type.REALTIME, range.start.getTime(), data);
        ds.persist("bugs-bugs", summary);

        summary = new Summary(Summary.Type.REALTIME, range.end.getTime() + 1, data);
        ds.persist("bugs-bugs", summary);

        // Read the summary and make sure that we don't summarize on the monthly level yet since it hasn't ended.
        MetricReader reader = new MetricReader();
        Object result = reader.read("bugs-bugs", "views", Summary.Type.MONTHLY, range, ds, new JsonPreprocessor(), new Sum());

        MapDataStore mds = (MapDataStore)ds;
        Assert.assertFalse(mds.data.containsKey(Summary.Type.MONTHLY));

        Assert.assertEquals(new BigDecimal("9223372036854775807"), result);
    }

    @Test
    public void testDontSummarizeForRangesThatIncludeToday() throws Exception
    {
        DataStore ds = DataStoreFactory.get();

        Map<String, String> data = new HashMap<String, String>();
        data.put("views", "1");
        data.put("hits", "123");

        // Create a new date range that includes today.
        DateRange range = DateRange.create(Summary.Type.MONTHLY, new Date());

        // Create a summary that's within that date range
        Summary summary = new Summary(Summary.Type.DAILY, range.start.getTime(), data);
        ds.persist("bugs-bugs", summary);

        // Read the summary and make sure that we don't summarize on the monthly level yet since it hasn't ended.
        MetricReader reader = new MetricReader();
        Object result = reader.read("bugs-bugs", "views", Summary.Type.MONTHLY, range, ds, new JsonPreprocessor(), new Sum());

        MapDataStore mds = (MapDataStore)ds;
        Assert.assertFalse(mds.data.containsKey(Summary.Type.MONTHLY));

        Assert.assertEquals(1, result);
    }

    @Test
    public void testDoSummarizeForRangesOtherThanTheOutermostIfTheyDontIncludeTodayButTheOuterDoes() throws Exception
    {
        DataStore ds = DataStoreFactory.get();

        Map<String, String> data = new HashMap<String, String>();
        data.put("views", "1");
        data.put("hits", "123");

        // Create a new date range that includes today.
        DateRange range = DateRange.create(Summary.Type.MONTHLY, new Date());

        // Create a summary that's within that date range
        Summary summary = new Summary(Summary.Type.REALTIME, range.start.getTime(), data);
        ds.persist("bugs-bugs", summary);

        // Read the summary and make sure that we don't summarize on the monthly level yet since it hasn't ended.
        MetricReader reader = new MetricReader();
        Object result = reader.read("bugs-bugs", "views", Summary.Type.MONTHLY, range, ds, new JsonPreprocessor(), new Sum());

        MapDataStore mds = (MapDataStore)ds;
        Assert.assertFalse(mds.data.containsKey(Summary.Type.MONTHLY));

        // Make sure we *do* summarize on the daily level though.
        Assert.assertTrue(mds.data.containsKey(Summary.Type.DAILY));

        Assert.assertEquals(1, result);
    }

    @Test
    public void testReadNone() throws Exception
    {
        DataStore ds = DataStoreFactory.get();

        MetricReader reader = new MetricReader();
        Object result = reader.read("bugs-bugs", "views", Summary.Type.DAILY, DateRange.create(Summary.Type.DAILY, new Date()), ds, new JsonPreprocessor(), new Sum());

        Assert.assertEquals(0, result);
    }

    @Test
    public void testReadSimple() throws Exception
    {
        DataStore ds = DataStoreFactory.get();

        Map<String, String> data = new HashMap<String, String>();
        data.put("views", "1");
        data.put("hits", "123");

        DateRange range = DateRange.create(Summary.Type.DAILY, new Date(0));
        Summary summary = new Summary(Summary.Type.DAILY, range.start.getTime(), data);
        ds.persist("bugs-bugs", summary);

        summary = new Summary(Summary.Type.DAILY, range.end.getTime() + 1, data);
        ds.persist("bugs-bugs", summary);

        MetricReader reader = new MetricReader();
        Object result = reader.read("bugs-bugs", "views", Summary.Type.DAILY, range, ds, new JsonPreprocessor(), new Sum());

        Assert.assertEquals(1, result);
    }

    @Test
    public void testReadCacheNew() throws Exception
    {
        DataStore ds = DataStoreFactory.get();

        Map<String, String> data = new HashMap<String, String>();
        data.put("views", "1");
        data.put("hits", "123");

        DateRange range = DateRange.create(Summary.Type.DAILY, new Date(0));
        Summary summary = new Summary(Summary.Type.REALTIME, range.start.getTime(), data);
        ds.persist("bugs-bugs", summary);

        summary = new Summary(Summary.Type.REALTIME, range.start.getTime() + 1, data);
        ds.persist("bugs-bugs", summary);

        MetricReader reader = new MetricReader();
        Object result = reader.read("bugs-bugs", "views", Summary.Type.DAILY, range, ds, new JsonPreprocessor(), new Sum());

        Assert.assertEquals(2, result);
    }
}
