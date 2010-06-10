package com.socrata.balboa.metrics.measurements.combining;

public class Count implements Combinator<Integer>
{
    Integer current = 0;

    @Override
    public Integer combine(Integer first, Integer second)
    {
        return first + second;
    }

    @Override
    public Integer feed(Integer input)
    {
        current += input;
        return current;
    }

    @Override
    public Integer getValue()
    {
        return current;
    }
}
