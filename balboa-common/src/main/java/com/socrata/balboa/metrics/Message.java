package com.socrata.balboa.metrics;

import java.io.IOException;

public abstract class Message
{
    String version;
    
    String entityId;
    long timestamp;
    Metrics metrics;

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

    public Metrics getMetrics()
    {
        return metrics;
    }

    public void setMetrics(Metrics metrics)
    {
        this.metrics = metrics;
    }

    public void put(String name, Metric metric)
    {
        getMetrics().put(name, metric);
    }

    public abstract byte[] serialize() throws IOException;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Message message = (Message) o;
        if (timestamp != message.timestamp) return false;
        if (entityId != null ? !entityId.equals(message.entityId) : message.entityId != null) return false;
        if (metrics != null ? !metrics.equals(message.metrics) : message.metrics != null) return false;
        if (version != null ? !version.equals(message.version) : message.version != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = version != null ? version.hashCode() : 0;
        result = 31 * result + (entityId != null ? entityId.hashCode() : 0);
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (metrics != null ? metrics.hashCode() : 0);
        return result;
    }
}
