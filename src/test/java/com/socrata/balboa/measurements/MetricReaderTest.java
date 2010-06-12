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
