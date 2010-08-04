package com.socrata.balboa.metrics.data;

import java.io.IOException;

public interface Lock
{
    public boolean acquire(String name) throws IOException;
    public void release(String name) throws IOException;
}
