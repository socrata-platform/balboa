package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.data.DateRange;
import com.yammer.metrics.core.MeterMetric;
import com.yammer.metrics.core.TimerMetric;
import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import org.apache.cassandra.thrift.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of CassandraQuery that actually speaks with cassandra.
 */
public class CassandraQueryImpl implements CassandraQuery
{
    private static Log log = LogFactory.getLog(CassandraQueryImpl.class);

    public static final TimerMetric persistMeter = com.yammer.metrics.Metrics.newTimer(CassandraQueryImpl.class, "total persist time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    public static final TimerMetric queryMeter = com.yammer.metrics.Metrics.newTimer(CassandraQueryImpl.class, "total query time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    public static final MeterMetric errorMeter = com.yammer.metrics.Metrics.newMeter(CassandraQueryImpl.class, "total errors in all queries (read & write)", "errors", TimeUnit.SECONDS);

    private static final int CLIENT_BORROW_THRESHOLD = 1000;

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

    CassandraClient getClient()
    {
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
            throw new CassandraDataStore.CassandraQueryException("Unknown exception trying to borrow a cassandra client.", e);
        }

        return client;
    }

    void releaseClient(CassandraClient client) throws IOException
    {
        try
        {
            pool.releaseClient(client);
        }
        catch (Exception e)
        {
            log.warn("Unable to release the cassandra client for some reason (not good).", e);
            throw new IOException(e);
        }
    }

    @Override
    public List<Column> getMeta(String entityId) throws IOException
    {
        long startTime = System.currentTimeMillis();

        CassandraClient hectorClient = getClient();
        Cassandra.Client client = hectorClient.getCassandra();

        try
        {
            SlicePredicate predicate = new SlicePredicate();
            SliceRange range = new SliceRange("".getBytes(), "".getBytes(), false, 500000);
            predicate.setSlice_range(range);
            List<Column> results = new ArrayList<Column>();

            while (true)
            {
                List<ColumnOrSuperColumn> queryResults = client.get_slice(keyspaceName, entityId, new ColumnParent("meta"), predicate, ConsistencyLevel.QUORUM);
                if (queryResults == null)
                {
                    break;
                }

                List<Column> columns = new ArrayList<Column>(queryResults.size());
                for (ColumnOrSuperColumn columnOrSuperColumn : queryResults)
                {
                    columns.add(columnOrSuperColumn.getColumn());
                }

                // Update the range so that the next time we fill the buffer, we
                // do it starting from the last of the returned results.
                if (queryResults.size() > 1)
                {
                    Column last = columns.remove(columns.size() - 1);
                    range.setStart(last.getName());
                    results.addAll(columns);
                }
                else if (queryResults.size() == 1)
                {
                    results.addAll(columns);
                    break;
                }
                else
                {
                    break;
                }
            }

            return results;
        }
        catch (InvalidRequestException e)
        {
            // The meta column doesn't currently exist, which is fine, we just
            // need to return and empty mock-meta
            return new ArrayList<Column>(0);
        }
        catch (Exception e)
        {
            errorMeter.mark();
            hectorClient.markAsError();
            throw new IOException("Unknown exception reading from cassandra.", e);
        }
        finally
        {
            long totalTime = System.currentTimeMillis() - startTime;
            log.debug("Total read time for entity meta on '" + entityId + "' to cassandra " + totalTime + " (ms).");

            releaseClient(hectorClient);
        }
    }

    @Override
    public List<SuperColumn> find(String entityId, SlicePredicate predicate, DateRange.Period period) throws IOException
    {
        long startTime = System.currentTimeMillis();
        CassandraClient hectorClient = getClient();
        Cassandra.Client client = hectorClient.getCassandra();

        try
        {
            List<ColumnOrSuperColumn> results = client.get_slice(keyspaceName, entityId, new ColumnParent(period.toString()), predicate, ConsistencyLevel.QUORUM);
            List<SuperColumn> superColumns = new ArrayList<SuperColumn>(results.size());

            for (ColumnOrSuperColumn result : results)
            {
                superColumns.add(result.getSuper_column());
            }

            return superColumns;
        }
        catch (TimedOutException e)
        {
            errorMeter.mark();
            hectorClient.markAsError();
            throw new IOException("Timeout exception reading from cassandra.", e);
        }
        catch (Exception e)
        {
            errorMeter.mark();
            hectorClient.markAsError();
            throw new IOException("Unknown exception reading from cassandra.", e);
        }
        finally
        {
            long totalTime = System.currentTimeMillis() - startTime;
            queryMeter.update(totalTime, TimeUnit.MILLISECONDS);
            log.debug("Total read time for '" + entityId + "' (" + period + ") to cassandra " + totalTime + " (ms).");

            releaseClient(hectorClient);
        }
    }

    @Override
    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> operations) throws IOException
    {
        long startTime = System.currentTimeMillis();
        CassandraClient hectorClient = getClient();
        Cassandra.Client client = hectorClient.getCassandra();

        try
        {
            client.batch_insert(keyspaceName, entityId, operations, ConsistencyLevel.QUORUM);
        }
        catch (Exception e)
        {
            errorMeter.mark();
            hectorClient.markAsError();
            throw new IOException("Unknown exception persisting to cassandra.", e);
        }
        finally
        {
            long totalTime = System.currentTimeMillis() - startTime;
            persistMeter.update(totalTime, TimeUnit.MILLISECONDS);
            log.debug("Total write time for '" + entityId + "' (all periods and meta, not including lock) to cassandra " + totalTime + " (ms)");

            releaseClient(hectorClient);
        }
    }

    @Override
    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException
    {
        long startTime = System.currentTimeMillis();

        CassandraClient hectorClient = getClient();
        Cassandra.Client client = hectorClient.getCassandra();

        try
        {
            SlicePredicate predicate = new SlicePredicate();
            SliceRange sliceRange = new SliceRange();
            sliceRange.setCount(0);
            sliceRange.setStart(CassandraUtils.packLong(0));
            sliceRange.setFinish(CassandraUtils.packLong(0));
            predicate.setSlice_range(sliceRange);

            return client.get_range_slices(
                    keyspaceName,
                    new ColumnParent(columnFamily),
                    predicate,
                    range,
                    ConsistencyLevel.ONE
            );
        }
        catch (Exception e)
        {
            errorMeter.mark();
            hectorClient.markAsError();
            throw new IOException("Unknown exception reading from cassandra.", e);
        }
        finally
        {
            long totalTime = System.currentTimeMillis() - startTime;
            log.debug("Total read time for keyscan between " + range.getStart_key() + " and " + range.getEnd_key() + " to cassandra " + totalTime + " (ms).");

            releaseClient(hectorClient);
        }
    }
}
