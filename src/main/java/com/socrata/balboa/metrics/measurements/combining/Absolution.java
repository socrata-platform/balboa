package com.socrata.balboa.metrics.measurements.combining;

public class Absolution implements Combinator<Number>
{
    @Override
    public Number combine(Number first, Number second)
    {
        if (second != null)
        {
            return second;
        }
        else
        {
            return first;
        }
    }
}
