package com.socrata.balboa.metrics.measurements.combining;

public class Sum implements Combinator<Integer>
{
    @Override
    public Integer combine(Integer first, Integer second)
    {
        if (first == null)
        {
            first = 0;
        }
        
        if (second == null)
        {
            second = 0;
        }

        return first + second;
    }
}
