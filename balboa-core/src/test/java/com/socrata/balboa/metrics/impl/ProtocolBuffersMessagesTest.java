package com.socrata.balboa.metrics.impl;

import com.socrata.balboa.metrics.Message;
import com.socrata.balboa.metrics.Metric;
import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

public class ProtocolBuffersMessagesTest
{
    @Test
    public void testSerializeEmpty() throws Exception
    {
        ProtocolBuffersMessages messages = new ProtocolBuffersMessages();
        byte[] data = messages.serialize();

        ProtocolBuffersMessages d = new ProtocolBuffersMessages(data);

        Assert.assertEquals(0, d.size());
    }

    @Test
    public void testSerializeNonEmpty() throws Exception
    {
        ProtocolBuffersMessages messages = new ProtocolBuffersMessages();

        long timestamp = new Date().getTime();
        Message message = new ProtocolBuffersMessage();
        message.setEntityId("saf");
        message.setTimestamp(timestamp);
        message.put("testing", new Metric(Metric.RecordType.AGGREGATE, 10));

        messages.add(message);

        message = new ProtocolBuffersMessage();
        message.setEntityId("samfoolio");
        message.setTimestamp(timestamp);
        message.put("sams-rating", new Metric(Metric.RecordType.AGGREGATE, 11));

        messages.add(message);

        byte[] data = messages.serialize();

        ProtocolBuffersMessages d = new ProtocolBuffersMessages(data);

        Assert.assertEquals(2, d.size());
        Assert.assertEquals("saf", d.get(0).getEntityId());
        Assert.assertEquals("samfoolio", d.get(1).getEntityId());
        Assert.assertEquals(11, d.get(1).getMetrics().get("sams-rating").getValue());
    }

    @Test(expected= IOException.class)
    public void invalidMessageCrashesSerialization() throws Exception
    {
        ProtocolBuffersMessages messages = new ProtocolBuffersMessages();
        Message m = new ProtocolBuffersMessage();
        messages.add(m);

        messages.serialize();
    }
}
