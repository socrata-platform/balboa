package com.socrata.balboa.metrics.measurements.preprocessing;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class JsonPreprocessor implements Preprocessor<Object>
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
