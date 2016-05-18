package com.socrata.balboa.metrics.measurements.combining;

public interface Combinator<T>
{
    T combine(T first, T second);
}
