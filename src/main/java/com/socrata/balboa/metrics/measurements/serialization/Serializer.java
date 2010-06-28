package com.socrata.balboa.metrics.measurements.serialization;

import java.io.IOException;

public interface Serializer<T>
{
    public String toString(T value) throws IOException;
    public T toValue(String serialized) throws IOException;
}
