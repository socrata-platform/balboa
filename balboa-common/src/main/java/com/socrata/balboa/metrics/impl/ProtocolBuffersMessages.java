package com.socrata.balboa.metrics.impl;

import com.socrata.balboa.metrics.Message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProtocolBuffersMessages extends ArrayList<Message>
{
    public ProtocolBuffersMessages()
    {
        super();
    }

    public ProtocolBuffersMessages(byte[] data) throws IOException
    {
        deserialize(MessageProtos.PBMessages.parseFrom(data));
    }

    public ProtocolBuffersMessages(MessageProtos.PBMessages serialized) throws IOException
    {
        deserialize(serialized);
    }

    void deserialize(MessageProtos.PBMessages serialized) throws IOException
    {
        for (MessageProtos.PBMessage message : serialized.getMessagesList())
        {
            add(new ProtocolBuffersMessage(message));
        }
    }

    public MessageProtos.PBMessages proto() throws IOException
    {
        List<MessageProtos.PBMessage> children = new ArrayList<>(size());

        for (Message message : this)
        {
            children.add(new ProtocolBuffersMessage(message).proto());
        }

        return MessageProtos.
                PBMessages.
                newBuilder().
                addAllMessages(children).
                build();
    }

    public byte[] serialize() throws IOException
    {
        return proto().toByteArray();
    }
}
