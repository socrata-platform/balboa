package com.socrata.balboa.metrics.impl;

import com.socrata.balboa.common.Message;
import com.socrata.balboa.common.Metrics;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class JsonMessage extends Message
{
    public JsonMessage()
    {
        super();
    }

    public JsonMessage(String serialized) throws IOException
    {
        super();
        deserialize(serialized);
    }

    @JsonProperty
    @Override
    public String getEntityId()
    {
        return super.getEntityId();
    }

    @JsonProperty
    @Override
    public long getTimestamp()
    {
        return super.getTimestamp();
    }

    @JsonProperty
    @Override
    public Metrics getMetrics()
    {
        return super.getMetrics();
    }

    @Override
    public byte[] serialize() throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        mapper.writeValue(stream, this);

        return stream.toByteArray();
    }

    void deserialize(String serialized) throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        JsonMessage other = mapper.readValue(serialized, JsonMessage.class);
        setEntityId(other.getEntityId());
        setMetrics(other.getMetrics());
        setTimestamp(other.getTimestamp());
    }

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (IOException e) {
            // For to String fail silently
            return "JsonMessage{}";
        }
    }
}
