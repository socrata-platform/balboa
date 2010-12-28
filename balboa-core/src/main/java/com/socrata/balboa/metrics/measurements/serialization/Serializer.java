package com.socrata.balboa.metrics.measurements.serialization;

import java.io.IOException;

public interface Serializer<T>
{
    public byte[] serialize(Object value) throws IOException;
    public Object deserialize(byte[] serialized) throws IOException;
}
