package com.socrata.balboa.metrics.impl;

import com.socrata.balboa.metrics.Metric;
import com.socrata.balboa.metrics.Metrics;
import junit.framework.Assert;
import org.junit.Test;

public class JsonMessageTest
{
    @Test
    public void testSerializeSimple() throws Exception
    {
        JsonMessage message = new JsonMessage();
        message.setEntityId("sam-foo");
        message.setTimestamp(0);

        Metrics metrics = new Metrics();
        metrics.put("foo", new Metric(Metric.RecordType.AGGREGATE, 100));

        message.setMetrics(metrics);

        byte[] json = message.serialize();

        JsonMessage message2 = new JsonMessage(new String(json));

        Assert.assertEquals("sam-foo", message2.getEntityId());
        Assert.assertEquals(0, message2.getTimestamp());
        Assert.assertTrue(message2.getMetrics().containsKey("foo"));
        Assert.assertEquals(100, message2.getMetrics().get("foo").getValue());
        Assert.assertEquals(Metric.RecordType.AGGREGATE, message2.getMetrics().get("foo").getType());
    }
}
