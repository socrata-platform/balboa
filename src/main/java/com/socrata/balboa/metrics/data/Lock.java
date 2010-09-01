package com.socrata.balboa.metrics.data;

import java.io.IOException;

/**
 * A simple lock interface that can be used by any DataStore to guarantee that
 * the key it's writing to isn't modified concurrently.
 * 
 * A lock should guarantee globally (across nodes) that a particular key can
 * only be acquired by one process at a time.
 */
public interface Lock
{
    public boolean acquire(String name) throws IOException;
    public void release(String name) throws IOException;
    public int delay();
}
