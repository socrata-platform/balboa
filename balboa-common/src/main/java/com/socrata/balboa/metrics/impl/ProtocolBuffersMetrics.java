package com.socrata.balboa.metrics.impl;

import com.socrata.balboa.common.Metric;
import com.socrata.balboa.common.Metrics;
import com.socrata.balboa.metrics.measurements.serialization.ProtocolBuffersSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ProtocolBuffersMetrics extends Metrics
{
    public ProtocolBuffersMetrics()
    {
        super();
    }

    public ProtocolBuffersMetrics(Metrics other)
    {
        super();

        putAll(other);
    }

    void deserialize(MessageProtos.PBMetrics serialized) throws IOException
    {
        ProtocolBuffersSerializer ser = new ProtocolBuffersSerializer();
        for (MessageProtos.PBMetric metric : serialized.getMetricsList())
        {
            put(
                    metric.getName(),
                    new Metric(
                            ProtocolBuffersMessage.protoToRecordType(metric.getType()),
                            ser.java(metric.getValue())
                    )
            );
        }
    }

    public MessageProtos.PBMetrics proto() throws IOException
    {
        List<MessageProtos.PBMetric> metrics = new ArrayList<MessageProtos.PBMetric>(size());

        ProtocolBuffersSerializer ser = new ProtocolBuffersSerializer();
        for (Map.Entry<String, Metric> entry : entrySet())
        {
            MessageProtos.PBMetric metric = MessageProtos.
                    PBMetric.
                    newBuilder().
                    setName(entry.getKey()).
                    setType(ProtocolBuffersMessage.recordToProtoType(entry.getValue().getType())).
                    setValue(ser.proto(entry.getValue().getValue())).
                    build();

            metrics.add(metric);
        }

        return MessageProtos.
                PBMetrics.
                newBuilder().
                addAllMetrics(metrics).
                build();
    }

    public byte[] serialize() throws IOException
    {
        return proto().toByteArray();
    }
}
