package com.socrata.balboa.metrics.measurements.serialization;

import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.config.ConfigurationException;

import java.io.IOException;

public class SerializerFactory
{
    public static Serializer get()
    {
        String serializerConfig;
        
        try
        {
            Configuration config = Configuration.get();
            serializerConfig = config.getProperty("balboa.serializer");
        }
        catch (IOException e)
        {
            throw new ConfigurationException("Serializer not configured.");
        }

        if (serializerConfig.equals("json"))
        {
            return new JsonSerializer();
        }
        else if (serializerConfig.equals("protobuf"))
        {
            return new ProtocolBuffersSerializer();
        }
        else
        {
            throw new ConfigurationException("Invalid serializer configured '" + serializerConfig + "'.");
        }
    }
}
