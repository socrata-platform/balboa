package com.socrata.balboa.metrics.messaging.impl;

import com.socrata.balboa.metrics.Summary;
import com.socrata.balboa.metrics.messaging.Receiver;

import java.util.ArrayList;
import java.util.List;

public class ListReceiver implements Receiver
{
    static class Message
    {
        public String entityId;
        public Summary summary;

        Message(String entityId, Summary summary)
        {
            this.entityId = entityId;
            this.summary = summary;
        }
    }

    List<Message> list = new ArrayList<Message>();

    @Override
    public void received(String entityId, Summary summary)
    {
        list.add(new Message(entityId, summary));
    }
}
