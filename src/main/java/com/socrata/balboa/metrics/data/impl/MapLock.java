package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.data.Lock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Don't use me. Seriously. Don Quixote will kick your ass if you use me.
 */
public class MapLock implements Lock
{
    public static Map<String, String> locks = new HashMap<String, String>();

    UUID id = UUID.randomUUID();

    @Override
    public boolean acquire(String name) throws IOException
    {
        if (locks.containsKey(name))
        {
            return false;
        }
        else
        {
            locks.put(name, id.toString());
            return true;
        }
    }

    @Override
    public void release(String name) throws IOException
    {
        if (!locks.get(name).equals(id.toString()))
        {
            throw new IOException("I don't own this lock.");
        }
        else
        {
            locks.remove(name);
        }
    }
}
