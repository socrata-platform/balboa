package com.socrata.balboa.metrics;

import java.io.IOException;
import java.util.Map;

public abstract class Message
{
    String version;
    
    String entityId;
    long timestamp;
    Map<String, Metric> metrics;

    public String getEntityId()
    {
        return entityId;
    }

    public void setEntityId(String entityId)
    {
        this.entityId = entityId;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    public Map<String, Metric> getMetrics()
    {
        return metrics;
    }

    public void setMetrics(Map<String, Metric> metrics)
    {
        this.metrics = metrics;
    }

    public void put(String name, Metric metric)
    {
        getMetrics().put(name, metric);
    }

    public abstract byte[] serialize() throws IOException;
}
