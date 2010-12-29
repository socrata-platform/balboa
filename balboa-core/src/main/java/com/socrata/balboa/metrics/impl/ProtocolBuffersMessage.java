package com.socrata.balboa.metrics.impl;

import com.socrata.balboa.metrics.Message;
import com.socrata.balboa.metrics.Metric;
import com.socrata.balboa.metrics.Metrics;
import com.socrata.balboa.metrics.measurements.serialization.ProtocolBuffersSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ProtocolBuffersMessage extends Message
{
    static final MessageProtos.PBMessage.Version CURRENT = MessageProtos.PBMessage.Version.V1_0;

    public ProtocolBuffersMessage()
    {
        setMetrics(new Metrics());
    }

    public ProtocolBuffersMessage(byte[] data) throws IOException
    {
        deserialize(MessageProtos.PBMessage.parseFrom(data));
    }

    public ProtocolBuffersMessage(MessageProtos.PBMessage serialized) throws IOException
    {
        deserialize(serialized);
    }

    public ProtocolBuffersMessage(Message existing)
    {
        setEntityId(existing.getEntityId());
        setTimestamp(existing.getTimestamp());
        setMetrics(existing.getMetrics());
    }

    static MessageProtos.PBMetric.Type recordToProtoType(Metric.RecordType type)
    {
        switch (type)
        {
            case AGGREGATE:
                return MessageProtos.PBMetric.Type.AGGREGATE;

            case ABSOLUTE:
                return MessageProtos.PBMetric.Type.ABSOLUTE;

            default:
                throw new IllegalArgumentException("Unsupported metric type '" + type + "'.");
        }
    }

    static Metric.RecordType protoToRecordType(MessageProtos.PBMetric.Type type)
    {
        switch (type)
        {
            case AGGREGATE:
                return Metric.RecordType.AGGREGATE;

            case ABSOLUTE:
                return Metric.RecordType.ABSOLUTE;

            default:
                throw new IllegalArgumentException("Unsupported metric type '" + type + "'.");
        }
    }

    Iterable<? extends MessageProtos.PBMetric> protoMetrics()
    {
        List<MessageProtos.PBMetric> serializableMetrics =
                new ArrayList<MessageProtos.PBMetric>(getMetrics().size());
        ProtocolBuffersSerializer numbersSer = new ProtocolBuffersSerializer();

        for (Map.Entry<String, Metric> metric : getMetrics().entrySet())
        {
            MessageProtos.PBMetric.Type type = recordToProtoType(metric.getValue().getType());

            MessageProtos.PBMetric protoBuf = MessageProtos.PBMetric.
                    newBuilder().
                    setName(metric.getKey()).
                    setType(type).
                    setValue(numbersSer.proto(metric.getValue().getValue())).
                    build();

            serializableMetrics.add(protoBuf);
        }

        return serializableMetrics;
    }

    public MessageProtos.PBMessage proto() throws IOException
    {
        if (getEntityId() == null)
        {
            throw new IOException("entityId is required.");
        }
        
        return MessageProtos.PBMessage.
                newBuilder().
                setEntityId(getEntityId()).
                setTimestamp(getTimestamp()).
                setVersion(CURRENT).
                addAllMetrics(protoMetrics()).
                build();
    }

    @Override
    public byte[] serialize() throws IOException
    {
        MessageProtos.PBMessage message = proto();

        return message.toByteArray();
    }

    void deserialize(MessageProtos.PBMessage serialized) throws IOException
    {
        setEntityId(serialized.getEntityId());
        setTimestamp(serialized.getTimestamp());

        ProtocolBuffersSerializer ser = new ProtocolBuffersSerializer();
        Metrics metrics = new Metrics(serialized.getMetricsCount());
        setMetrics(metrics);

        for (MessageProtos.PBMetric metric : serialized.getMetricsList())
        {
            Metric m = new Metric(
                    protoToRecordType(metric.getType()),
                    ser.java(metric.getValue())
            );

            metrics.put(metric.getName(), m);
        }
    }
}

