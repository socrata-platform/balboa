package com.socrata.balboa.metrics.measurements.serialization;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class JsonSerializer implements Serializer<Object>
{
    @Override
    public byte[] serialize(Object value) throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        mapper.writeValue(stream, value);

        return stream.toByteArray();
    }

    @Override
    public Object deserialize(byte[] serialized) throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();

        return mapper.readValue(serialized.toString(), Object.class);
    }
}
