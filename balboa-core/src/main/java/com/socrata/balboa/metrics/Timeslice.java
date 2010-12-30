package com.socrata.balboa.metrics;

public class Timeslice
{
    long start;
    long end;
    Metrics metrics;

    public long getStart()
    {
        return start;
    }

    public void setStart(long start)
    {
        this.start = start;
    }

    public long getEnd()
    {
        return end;
    }

    public void setEnd(long end)
    {
        this.end = end;
    }

    public Metrics getMetrics()
    {
        return metrics;
    }

    public void setMetrics(Metrics metrics)
    {
        this.metrics = metrics;
    }
}
