package com.socrata.balboa.metrics.impl;

import com.socrata.balboa.metrics.Metric;
import com.socrata.balboa.metrics.Metrics;
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

        setTimestamp(other.getTimestamp());
        getMetrics().putAll(other.getMetrics());
    }

    void deserialize(MessageProtos.PBMetrics serialized) throws IOException
    {
        ProtocolBuffersSerializer ser = new ProtocolBuffersSerializer();
        for (MessageProtos.PBMetric metric : serialized.getMetricsList())
        {
            getMetrics().put(
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
        List<MessageProtos.PBMetric> metrics = new ArrayList<MessageProtos.PBMetric>(getMetrics().size());

        ProtocolBuffersSerializer ser = new ProtocolBuffersSerializer();
        for (Map.Entry<String, Metric> entry : getMetrics().entrySet())
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
                setTimestamp(getTimestamp()).
                build();
    }

    public byte[] serialize() throws IOException
    {
        return proto().toByteArray();
    }
}
