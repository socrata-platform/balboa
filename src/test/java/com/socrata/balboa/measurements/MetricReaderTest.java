package com.socrata.balboa.measurements;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.metrics.measurements.MetricReader;
import com.socrata.balboa.metrics.measurements.combining.Sum;
import com.socrata.balboa.metrics.measurements.preprocessing.JsonPreprocessor;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class MetricReaderTest
{
    @Test
    public void testRead() throws Exception
    {
        DataStore ds = DataStoreFactory.get();

        Map<String, String> data = new HashMap<String, String>();
        data.put("views", "1");
        data.put("hits", "123");

        DateRange range = DateRange.create(Summary.Type.MONTHLY, new Date(0));
        Summary summary = new Summary(Summary.Type.REALTIME, range.start.getTime(), data);
        ds.persist("bugs-bugs", summary);

        summary = new Summary(Summary.Type.REALTIME, range.end.getTime() + 1, data);
        ds.persist("bugs-bugs", summary);

        Object result = MetricReader.read(
              "bugs-bugs",
              "views",
              DateRange.create(Summary.Type.MONTHLY, new Date(0)).start,
              new Date(),
              new JsonPreprocessor(),
              new Sum()
        );

        Assert.assertEquals(2, result);
    }
}
