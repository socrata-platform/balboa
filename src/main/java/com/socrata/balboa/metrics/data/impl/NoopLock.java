package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.data.Lock;

import java.io.IOException;

public class NoopLock implements Lock
{
    @Override
    public boolean acquire(String name) throws IOException
    {
        // Lockpicking special, you can always lock anything!
        return true;
    }

    @Override
    public void release(String name) throws IOException
    {
    }
}
