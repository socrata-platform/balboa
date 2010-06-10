package com.socrata.balboa.metrics.measurements;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.Summary.Type;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import com.socrata.balboa.metrics.measurements.combining.Combinator;
import com.socrata.balboa.metrics.measurements.preprocessing.Preprocessor;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

public class MetricReader
{
    public static Object read(String entityId, String field, Date start, Date end, Preprocessor preprocessor, Combinator combinator) throws IOException
    {
        DataStore ds = DataStoreFactory.get();

        Iterator<Summary> iter = ds.find(entityId, Type.REALTIME, start, end);

        while (iter.hasNext())
        {
            Summary summary = iter.next();

            if (summary.getValues().containsKey(field))
            {
                String serializedValue = summary.getValues().get(field);

                Object value = preprocessor.toValue(serializedValue);
                combinator.feed(value);
            }
        }

        return combinator.getValue();
    }
}
