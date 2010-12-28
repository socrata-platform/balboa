package com.socrata.balboa.metrics.impl;

import com.socrata.balboa.metrics.Message;
import com.socrata.balboa.metrics.Metric;
import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ProtocolBuffersMessageTest
{
    @Test(expected=IOException.class)
    public void testInvalidSerialize() throws Exception
    {
        Message m = new ProtocolBuffersMessage();

        byte[] serialized = m.serialize();
    }

    @Test
    public void testSerialize() throws Exception
    {
        Message m = new ProtocolBuffersMessage();
        m.setEntityId("sam");
        m.setTimestamp(0);

        byte[] serialized = m.serialize();
        Message d = new ProtocolBuffersMessage(serialized);

        Assert.assertEquals("sam", d.getEntityId());
        Assert.assertEquals(0, d.getTimestamp());
    }

    @Test
    public void testSerializeMetrics() throws Exception
    {
        Message m = new ProtocolBuffersMessage();
        m.setEntityId("sam");
        m.setTimestamp(0);

        Map<String, Metric> metrics = new HashMap<String, Metric>();
        metrics.put("test", new Metric(Metric.RecordType.AGGREGATE, 10));
        metrics.put("test2", new Metric(Metric.RecordType.ABSOLUTE, 11));

        m.setMetrics(metrics);

        byte[] serialized = m.serialize();
        Message d = new ProtocolBuffersMessage(serialized);

        Assert.assertEquals("sam", d.getEntityId());
        Assert.assertEquals(0, d.getTimestamp());
        Assert.assertEquals(2, d.getMetrics().size());
        Assert.assertEquals(10, d.getMetrics().get("test").getValue());
        Assert.assertEquals(11, d.getMetrics().get("test2").getValue());
    }
}
