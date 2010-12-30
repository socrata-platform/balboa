package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.config.PropertiesConfiguration;
import com.socrata.balboa.metrics.data.impl.MapLock;
import com.socrata.balboa.metrics.data.impl.MemcachedLock;
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
    private static MemcachedClient client;

    static synchronized MemcachedClient getCacheClient() throws IOException
    {
        if (client == null)
        {
            Configuration config = Configuration.get();
            String serverConfig = config.getProperty("memcached.servers");

            if (serverConfig == null)
            {
                throw new PropertiesConfiguration.ConfigurationException("memcached.servers must be configured in order to enable locking.");
            }

            log.debug("Retrieving a MemcachedLock instance for the '" + serverConfig + "' memcache servers.");

            List<InetSocketAddress> address = AddrUtil.getAddresses(serverConfig);
            client = new MemcachedClient(address);
        }

        return client;
    }

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
                return new MemcachedLock(getCacheClient());
            }
            catch (IOException e)
            {
                log.fatal("Unable to read the configuration file to figure out what to connect to.", e);
                throw new PropertiesConfiguration.ConfigurationException("Unable to read the configuration file to figure out what to connect to.", e);
            }
        }
    }
}
