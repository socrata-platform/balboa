package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.config.PropertiesConfiguration;
import com.socrata.balboa.metrics.data.impl.MapLock;
import com.socrata.balboa.metrics.data.impl.MemcachedLock;
import com.socrata.balboa.metrics.data.impl.SingleHostLock;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public class LockFactory
{
    private static final Lock singleHostLock = new SingleHostLock(Integer.MAX_VALUE, 1000);
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
            client = new MemcachedClient(
                    new ConnectionFactoryBuilder().setDaemon(true).build(),
                    address
            );
        }

        return client;
    }

    public static Lock get() throws IOException
    {
        return singleHostLock;
    }
}
