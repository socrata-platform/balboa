package com.socrata.balboa.metrics.measurements.combining;

public interface Combinator<T>
{
    public T combine(T first, T second);
}
