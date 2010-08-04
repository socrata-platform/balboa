package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.data.Lock;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class MemcachedLock implements Lock
{
    private static Log log = LogFactory.getLog(MemcachedLock.class);

    // Set the lock timeout to 2 minutes.
    private static final int LOCK_TIMEOUT = 120;

    MemcachedClient client;
    UUID id = UUID.randomUUID();

    public MemcachedLock(String server) throws IOException
    {
        List<InetSocketAddress> address = AddrUtil.getAddresses(server);
        client = new MemcachedClient(address);
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
