package com.socrata.balboa.metrics.measurements.serialization;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class JsonSerializer implements Serializer<Object>
{
    @Override
    public String toString(Object value) throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        mapper.writeValue(stream, value);
        
        return stream.toString();
    }

    @Override
    public Object toValue(String serialized) throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();

        return mapper.readValue(serialized, Object.class);
    }
}
