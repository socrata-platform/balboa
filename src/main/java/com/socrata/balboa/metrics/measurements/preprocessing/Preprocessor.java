package com.socrata.balboa.metrics.measurements.preprocessing;

import java.io.IOException;

public interface Preprocessor<T>
{
    public String toString(T value) throws IOException;
    public T toValue(String serialized) throws IOException;
}
