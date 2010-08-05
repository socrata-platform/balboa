package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.data.Lock;
import net.spy.memcached.MemcachedClientIF;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * A simple distributed lock mechanism to allow multiple balboa "write" nodes to
 * run concurrently.
 *
 * Memcache guarantees that a call to "add" is atomic and will fail if the key
 * already exists in the cache. Because of this we can use it as a simple
 * distributed lock.
 *
 * WARNING: Technically it is possible for memcached to evict our lock key while
 * the lock is still held, causing our entire house of cards to come tumbling
 * down. However, in practice if the memcache server is evicting keys after
 * only two minutes, there's bigger problems. This can also be alleviated by
 * running a memcached cluster solely for the lock services.
 */
public class MemcachedLock implements Lock
{
    private static Log log = LogFactory.getLog(MemcachedLock.class);

    // Set the lock timeout to 2 minutes.
    private static final int LOCK_TIMEOUT = 120;

    MemcachedClientIF client;
    UUID id = UUID.randomUUID();

    public MemcachedLock(MemcachedClientIF client) throws IOException
    {
        this.client = client;
    }

    @Override
    public boolean acquire(String name) throws IOException
    {
        try
        {
            log.debug("Attempting to acquire lock 'balboa:lock:" + name + "'.");
            boolean lock = client.add("balboa:lock:" + name, LOCK_TIMEOUT, id.toString()).get();

            if (lock)
            {
                log.debug("Lock 'balboa:lock:" + name + "' acquired.");
            }
            else
            {
                log.debug("Lock 'balboa:lock:" + name + "' was not acquired.");
            }

            return lock;
        }
        catch (InterruptedException e)
        {
            log.fatal("Unable to determine status of lock and acquire it. Unable to get the results of a memcached add operation because the current thread was interrupted while waiting for a response from memcache.", e);
            throw new IOException(e);
        }
        catch (ExecutionException e)
        {
            log.fatal("Unable to determine status of lock and acquire it. There was some unknown exception waiting for a response from memcache.", e);
            throw new IOException(e);
        }
    }

    @Override
    public void release(String name) throws IOException
    {
        log.debug("Attempting to release lock 'balboa:lock:" + name + "'.");

        String ownerId = (String)client.get("balboa:lock:" + name);

        if (ownerId == null)
        {
            log.warn("There's not lock with the key '" + name + "' acquired. Ignoring.");
        }
        else
        {
            if (ownerId.equals(id.toString()))
            {
                client.delete("balboa:lock:" + name);
            }
            else
            {
                throw new IOException("The lock that I thought was mine turns out to have been someone else's. Not going to release it.");
            }
        }
    }
}
