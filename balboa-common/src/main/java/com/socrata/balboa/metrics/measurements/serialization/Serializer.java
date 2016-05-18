package com.socrata.balboa.metrics.measurements.serialization;

import java.io.IOException;

public interface Serializer<T>
{
    byte[] serialize(Object value) throws IOException;
    Object deserialize(byte[] serialized) throws IOException;
}
