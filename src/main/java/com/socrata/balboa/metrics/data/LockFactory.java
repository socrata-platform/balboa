package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.data.impl.MapLock;
import com.socrata.balboa.metrics.data.impl.MemcachedLock;
import com.socrata.balboa.server.exceptions.InternalException;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public class LockFactory
{
    private static Log log = LogFactory.getLog(LockFactory.class);

    public static Lock get() throws IOException
    {
        String environment = System.getProperty("socrata.env");

        if ("test".equals(environment))
        {
            log.debug("Retrieving a MapLock instance.");
            return new MapLock();
        }
        else
        {
            try
            {
                Configuration config = Configuration.get();
                String serverConfig = config.getProperty("memcached.servers");

                if (serverConfig == null)
                {
                    throw new InternalException("memcached.servers must be configured in order to enable locking.");
                }

                log.debug("Retrieving a MemcachedLock instance for the '" + serverConfig + "' memcache servers.");
                List<InetSocketAddress> address = AddrUtil.getAddresses(serverConfig);
                MemcachedClient client = new MemcachedClient(address);
                
                return new MemcachedLock(client);
            }
            catch (IOException e)
            {
                log.fatal("Unable to read the configuration file to figure out what to connect to.", e);
                throw new InternalException("Unable to read the configuration file to figure out what to connect to.", e);
            }
        }
    }
}
