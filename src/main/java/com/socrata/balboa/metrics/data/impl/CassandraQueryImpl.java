package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.data.DateRange;
import com.socrata.balboa.server.exceptions.InternalException;
import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.Keyspace;
import org.apache.cassandra.thrift.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * An implementation of CassandraQuery that actually speaks with cassandra.
 */
public class CassandraQueryImpl implements CassandraQuery
{
    private static Log log = LogFactory.getLog(CassandraQueryImpl.class);

    private static final int CLIENT_BORROW_THRESHOLD = 1000;
    private static final int KEYSPACE_BORROW_THRESHOLD = 1000;

    String keyspaceName;
    CassandraClientPool pool;
    String[] servers;

    public CassandraQueryImpl() throws IOException
    {
        Configuration config = Configuration.get();
        servers = config.getProperty("cassandra.servers").split(",");
        keyspaceName = config.getProperty("cassandra.keyspace");

        pool = CassandraClientPoolFactory.getInstance().get();
    }

    @Override
    public List<SuperColumn> find(String entityId, SlicePredicate predicate, DateRange.Type type) throws IOException
    {
        long startTime = System.currentTimeMillis();
        
        CassandraClient client;
        try
        {
            long clientStartTime = System.currentTimeMillis();
            client = pool.borrowClient(servers);
            long totalClientTime = System.currentTimeMillis() - clientStartTime;
            if (totalClientTime >= CLIENT_BORROW_THRESHOLD)
            {
                log.warn("Slow borrowing a client from the pool " + totalClientTime + " (ms).");
            }
        }
        catch (Exception e)
        {
            throw new InternalException("Unknown exception trying to borrow a cassandra client.", e);
        }

        Keyspace keyspace = null;

        try
        {
            long keyspaceStartTime = System.currentTimeMillis();
            keyspace = client.getKeyspace(keyspaceName, ConsistencyLevel.QUORUM, CassandraClient.FailoverPolicy.FAIL_FAST);
            long totalKeyspaceTime = System.currentTimeMillis() - keyspaceStartTime;
            if (totalKeyspaceTime >= KEYSPACE_BORROW_THRESHOLD)
            {
                log.warn("Slow getting a keyspace for reading from cassandra " + totalKeyspaceTime + " (ms).");
            }

            return keyspace.getSuperSlice(entityId, new ColumnParent(type.toString()), predicate);
        }
        catch (NotFoundException e)
        {
            throw new IOException("Keyspace '" + keyspaceName + "' not found.");
        }
        catch (Exception e)
        {
            throw new IOException("Unknown exception reading from cassandra.", e);
        }
        finally
        {
            IOException possiblyThrown = null;

            try
            {
                pool.releaseClient(keyspace == null ? client : keyspace.getClient());
            }
            catch (Exception e)
            {
                log.warn("Unable to release the cassandra client for some reason (not good).", e);
                possiblyThrown = new IOException(e);
            }

            long totalTime = System.currentTimeMillis() - startTime;
            log.debug("Total read time for '" + entityId + "' to cassandra " + totalTime + " (ms).");

            if (possiblyThrown != null)
            {
                // Doesn't really matter which one is thrown, we just want to
                // chuck out any exceptions and let someone smarter than us
                // deal with them.
                throw possiblyThrown;
            }
        }
    }

    @Override
    public void persist(String entityId, Map<String, List<SuperColumn>> superColumnOperations) throws IOException
    {
        long startTime = System.currentTimeMillis();
        
        CassandraClient client;
        try
        {
            client = pool.borrowClient(servers);
        }
        catch (Exception e)
        {
            throw new InternalException("Unknown exception trying to borrow a cassandra client.", e);
        }

        Keyspace keyspace = null;

        try
        {
            long keyspaceStartTime = System.currentTimeMillis();
            keyspace = client.getKeyspace(keyspaceName, ConsistencyLevel.QUORUM, CassandraClient.FailoverPolicy.FAIL_FAST);
            long totalKeyspaceTime = System.currentTimeMillis() - keyspaceStartTime;
            if (totalKeyspaceTime >= KEYSPACE_BORROW_THRESHOLD)
            {
                log.warn("Slow getting a keyspace for writing from cassandra " + totalKeyspaceTime + " (ms).");
            }
            keyspace.batchInsert(entityId, null, superColumnOperations);
        }
        catch (NotFoundException e)
        {
            throw new IOException("Keyspace '" + keyspaceName + "' not found.");
        }
        catch (Exception e)
        {
            throw new IOException("Unknown exception persisting to cassandra.", e);
        }
        finally
        {
            IOException possiblyThrown = null;

            try
            {
                pool.releaseClient(keyspace == null ? client : keyspace.getClient());
            }
            catch (Exception e)
            {
                log.warn("Unable to release the cassandra client for some reason (not good).", e);
                possiblyThrown = new IOException(e);
            }

            long totalTime = System.currentTimeMillis() - startTime;
            log.debug("Total write time for '" + entityId + "' to cassandra (not including lock) " + totalTime + " (ms)");

            if (possiblyThrown != null)
            {
                // Doesn't really matter which one is thrown, we just want to
                // chuck out any exceptions and let someone smarter than us
                // deal with them.
                throw possiblyThrown;
            }
        }
    }
}
