package com.socrata.balboa.metrics.impl;

import com.socrata.balboa.common.Message;

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
        List<MessageProtos.PBMessage> children = new ArrayList<MessageProtos.PBMessage>(size());

        for (Message message : this)
        {
            children.add(new ProtocolBuffersMessage(message).proto());
        }

        MessageProtos.PBMessages messages = MessageProtos.
                PBMessages.
                newBuilder().
                addAllMessages(children).
                build();
        
        return messages;
    }

    public byte[] serialize() throws IOException
    {
        return proto().toByteArray();
    }
}
