package com.socrata.balboa.metrics.measurements.combining;

import com.socrata.balboa.metrics.Summary;

public interface Combinator<T>
{
    public T combine(T first, T second);
    public T feed(T input);
    public T getValue();
}
