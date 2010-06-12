package com.socrata.balboa.metrics.measurements.combining;

public class Count implements Combinator<Integer>
{
    @Override
    public Integer combine(Integer first, Integer second)
    {
        return first + second;
    }
}
