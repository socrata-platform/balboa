package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.Metric;
import com.socrata.balboa.metrics.Metrics;
import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.data.*;
import com.socrata.balboa.metrics.measurements.serialization.SerializerFactory;
import org.apache.cassandra.thrift.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class CassandraDataStoreTest 
{
    public DataStore get()
    {
        return new CassandraDataStore();
    }

    @Before
    public void setup()
    {
        CassandraQueryFactory.setTestMock(null);
    }

    @Test
    public void testCantPersistUnderscoredColumns() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(Period.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, Period period) throws IOException
                    {
                        return new ArrayList<SuperColumn>(0);
                    }

                    @Override
                    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException { return null; }

                    @Override
                    public List<Column> getMeta(String entityId) throws IOException { return null; }

                    @Override
                    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> inserts) throws IOException {}
                }
        );

        Metrics data = new Metrics();
        data.put("__chompa__", new Metric(Metric.RecordType.AGGREGATE, 1));

        try
        {
            ds.persist("testCantPersistUnderscoredColumns", range.start.getTime(), data);
            Assert.fail();
        }
        catch (IllegalArgumentException e){}
    }

    @Test
    public void testCantPersistUnderscoredEntities() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(Period.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, Period period) throws IOException
                    {
                        return new ArrayList<SuperColumn>(0);
                    }

                    @Override
                    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException { return null; }

                    @Override
                    public List<Column> getMeta(String entityId) throws IOException { return null; }

                    @Override
                    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> inserts) throws IOException {}
                }
        );

        Metrics data = new Metrics();
        data.put("test1", new Metric(Metric.RecordType.AGGREGATE, 1));

        try
        {
            ds.persist("__illegal.key.name__", range.start.getTime(), data);
            Assert.fail();
        }
        catch (IllegalArgumentException e){}
    }

    @Test
    public void testReadIOError() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(Period.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, Period period) throws IOException
                    {
                        throw new IOException("Oh Cody. This is so wrong.");
                    }

                    @Override
                    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException { return null; }

                    @Override
                    public List<Column> getMeta(String entityId) throws IOException { return null; }

                    @Override
                    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Metrics> iter = ds.find("testRemoveFails", range.start, range.end);

        try
        {
            iter.hasNext();
            Assert.fail();
        }
        catch (CassandraDataStore.CassandraQueryException e) {}
    }

    @Test
    public void testRemoveFails() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(Period.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, Period period) throws IOException
                    {
                        long start = CassandraUtils.unpackLong(predicate.getSlice_range().getStart());
                        if (start > range.start.getTime())
                        {
                            return new ArrayList<SuperColumn>(0);
                        }

                        List<SuperColumn> items = new ArrayList<SuperColumn>(2);
                        List<Column> columns = new ArrayList<Column>(1);

                        columns.add(new Column("test1".getBytes(), SerializerFactory.get().serialize(1), 0));
                        columns.add(new Column("test2".getBytes(), SerializerFactory.get().serialize(2), 0));
                        items.add(new SuperColumn(CassandraUtils.packLong(range.start.getTime()), columns));

                        return items;
                    }

                    @Override
                    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException { return null; }

                    @Override
                    public List<Column> getMeta(String entityId) throws IOException { return null; }

                    @Override
                    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Metrics> iter = ds.find("testRemoveFails", range.start, range.end);

        Assert.assertTrue(iter.hasNext());

        try
        {
            iter.remove();
            Assert.fail();
        }
        catch (UnsupportedOperationException e) {}

    }

    @Test
    public void testOnAnOddTimeBoundaryTheSuperColumnNameIsForcedToTheRightBoundary() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(Period.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, Period period) throws IOException
                    {
                        return new ArrayList<SuperColumn>(0);
                    }

                    @Override
                    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException { return null; }

                    @Override
                    public List<Column> getMeta(String entityId) throws IOException { return null; }

                    @Override
                    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> inserts) throws IOException
                    {
                        String t = Period.HOURLY.toString();
                        
                        Assert.assertTrue(inserts.get(t).size() == 1);

                        ColumnOrSuperColumn insert = inserts.get(t).get(0);
                        long timestamp = CassandraUtils.unpackLong(insert.getSuper_column().getName());

                        Assert.assertNotSame(range.start.getTime() + 31337, timestamp);

                        DateRange boundary = DateRange.create(Period.HOURLY, new Date(timestamp));
                        Assert.assertEquals(boundary.start.getTime(), timestamp);
                    }
                }
        );

        Metrics data = new Metrics();
        data.put("test1", new Metric(Metric.RecordType.AGGREGATE, 1));
        data.put("test2", new Metric(Metric.RecordType.AGGREGATE, 2));

        ds.persist("testCreate", range.start.getTime() + 31337, data);
    }

    @Test
    public void testNoSummariesNextCallThrowsException() throws Exception
    {
        DataStore ds = get();
        DateRange range = DateRange.create(Period.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, Period period) throws IOException
                    {
                        return null;
                    }

                    @Override
                    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException { return null; }

                    @Override
                    public List<Column> getMeta(String entityId) throws IOException { return null; }

                    @Override
                    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Metrics> iter = ds.find("testRangeScan4", range.start, range.end);

        Assert.assertNotNull(iter);

        try
        {
            iter.next();
            Assert.fail();
        }
        catch (NoSuchElementException e) {}
    }

    @Test
    public void testInvalidlySerializedValueDoesNotCrash() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(Period.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, Period period) throws IOException
                    {
                        long start = CassandraUtils.unpackLong(predicate.getSlice_range().getStart());
                        if (start > range.start.getTime())
                        {
                            return new ArrayList<SuperColumn>(0);
                        }

                        List<SuperColumn> items = new ArrayList<SuperColumn>(1);
                        List<Column> columns = new ArrayList<Column>(1);

                        columns.add(new Column("test2".getBytes(), new byte[] {(byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef}, 0));
                        items.add(new SuperColumn(CassandraUtils.packLong(range.start.getTime()), columns));

                        return items;
                    }

                    @Override
                    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException { return null; }

                    @Override
                    public List<Column> getMeta(String entityId) throws IOException { return null; }

                    @Override
                    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Metrics> iter = ds.find("evil", range.start, range.end);

        Assert.assertTrue(iter.hasNext());

        Metrics sum = iter.next();

        Assert.assertEquals(0, sum.size());
    }

    @Test
    public void testNullResultMeansEmptySummary() throws Exception
    {
        DataStore ds = get();
        DateRange range = DateRange.create(Period.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, Period period) throws IOException
                    {
                        return null;
                    }

                    @Override
                    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException { return null; }

                    @Override
                    public List<Column> getMeta(String entityId) throws IOException { return null; }

                    @Override
                    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Metrics> iter = ds.find("testRangeScan4", range.start, range.end);

        Assert.assertNotNull(iter);
        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void testQueryWhichFillsUpBuffer() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(Period.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, Period period) throws IOException
                    {
                        long start = CassandraUtils.unpackLong(predicate.getSlice_range().getStart());
                        
                        if(start >= range.start.getTime() + CassandraDataStore.CassandraIterator.QUERYBUFFER + 1)
                        {
                            return new ArrayList<SuperColumn>(0);
                        }
                        else if (start >= range.start.getTime() + CassandraDataStore.CassandraIterator.QUERYBUFFER)
                        {
                            List<SuperColumn> items = new ArrayList<SuperColumn>(1);
                            List<Column> columns = new ArrayList<Column>(1);

                            columns.add(new Column("test2".getBytes(), SerializerFactory.get().serialize(2), 0));
                            items.add(new SuperColumn(CassandraUtils.packLong(range.start.getTime() + CassandraDataStore.CassandraIterator.QUERYBUFFER + 1), columns));

                            return items;
                        }
                        else
                        {
                            List<SuperColumn> items = new ArrayList<SuperColumn>(CassandraDataStore.CassandraIterator.QUERYBUFFER);
                            for (int i=0; i < CassandraDataStore.CassandraIterator.QUERYBUFFER; i++)
                            {
                                List<Column> columns = new ArrayList<Column>(1);

                                columns.add(new Column("test1".getBytes(), SerializerFactory.get().serialize(1), 0));
                                items.add(new SuperColumn(CassandraUtils.packLong(range.start.getTime() + i), columns));
                            }

                            return items;
                        }
                    }

                    @Override
                    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException { return null; }

                    @Override
                    public List<Column> getMeta(String entityId) throws IOException { return null; }

                    @Override
                    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Metrics> iter = ds.find("testRangeScan1", range.start, range.end);

        Assert.assertTrue(iter.hasNext());

        Metrics results = Metrics.summarize(iter);

        Assert.assertEquals(CassandraDataStore.CassandraIterator.QUERYBUFFER, results.get("test1").getValue());
        Assert.assertEquals(2, results.get("test2").getValue());

        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void testQueryWithMoreThanOneResult() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(Period.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, Period period) throws IOException
                    {
                        long start = CassandraUtils.unpackLong(predicate.getSlice_range().getStart());
                        if (start > range.start.getTime())
                        {
                            return new ArrayList<SuperColumn>(0);
                        }

                        List<SuperColumn> items = new ArrayList<SuperColumn>(2);
                        List<Column> columns = new ArrayList<Column>(2);

                        columns.add(new Column("test1".getBytes(), SerializerFactory.get().serialize(1), 0));
                        columns.add(new Column("test2".getBytes(), SerializerFactory.get().serialize(2), 0));
                        items.add(new SuperColumn(CassandraUtils.packLong(range.start.getTime()), columns));

                        columns = new ArrayList<Column>(1);
                        columns.add(new Column("test3".getBytes(), SerializerFactory.get().serialize(3), 0));
                        items.add(new SuperColumn(CassandraUtils.packLong(range.start.getTime() + 1), columns));

                        return items;
                    }

                    @Override
                    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException { return null; }

                    @Override
                    public List<Column> getMeta(String entityId) throws IOException { return null; }

                    @Override
                    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Metrics> iter = ds.find("testRangeScan1", range.start, range.end);

        Assert.assertTrue(iter.hasNext());

        Metrics results = Metrics.summarize(iter);

        Assert.assertEquals(1, results.get("test1").getValue());
        Assert.assertEquals(2, results.get("test2").getValue());
        Assert.assertEquals(3, results.get("test3").getValue());

        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void testRangeScanSimple() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(Period.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, Period period) throws IOException
                    {
                        long start = CassandraUtils.unpackLong(predicate.getSlice_range().getStart());
                        if (start > range.start.getTime())
                        {
                            return new ArrayList<SuperColumn>(0);
                        }

                        List<SuperColumn> items = new ArrayList<SuperColumn>(2);
                        List<Column> columns = new ArrayList<Column>(1);

                        columns.add(new Column("test1".getBytes(), SerializerFactory.get().serialize(1), 0));
                        columns.add(new Column("test2".getBytes(), SerializerFactory.get().serialize(2), 0));
                        items.add(new SuperColumn(CassandraUtils.packLong(range.start.getTime()), columns));

                        return items;
                    }

                    @Override
                    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException { return null; }

                    @Override
                    public List<Column> getMeta(String entityId) throws IOException { return null; }

                    @Override
                    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Metrics> iter = ds.find("testRangeScan1", range.start, range.end);

        Assert.assertTrue(iter.hasNext());

        Metrics results = Metrics.summarize(iter);

        Assert.assertEquals(1, results.get("test1").getValue());
        Assert.assertEquals(2, results.get("test2").getValue());

        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void testRangeScanNoItems() throws Exception
    {
        DataStore ds = get();
        DateRange range = DateRange.create(Period.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, Period period) throws IOException
                    {
                        return new ArrayList<SuperColumn>(0);
                    }

                    @Override
                    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException { return null; }

                    @Override
                    public List<Column> getMeta(String entityId) throws IOException { return null; }

                    @Override
                    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Metrics> iter = ds.find("testRangeScan4", range.start, range.end);

        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void testCreate() throws Exception
    {
        DataStore ds = get();

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, Period period) throws IOException
                    {
                        return new ArrayList<SuperColumn>(0);
                    }

                    @Override
                    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException { return null; }

                    @Override
                    public List<Column> getMeta(String entityId) throws IOException { return null; }

                    @Override
                    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> inserts) throws IOException
                    {
                        Assert.assertEquals("testCreate", entityId);
                        Assert.assertTrue(Configuration.get().getSupportedPeriods().size() > 0);

                        for (Period t : Configuration.get().getSupportedPeriods())
                        {
                            Assert.assertTrue(inserts.containsKey(t.toString()));
                        }

                        String t = Configuration.get().getSupportedPeriods().get(0).toString();
                        
                        Assert.assertTrue(inserts.get(t).size() == 1);

                        List<Column> cols = inserts.get(t).get(0).getSuper_column().getColumns();
                        Assert.assertTrue(cols.size() == 2);
                        Assert.assertTrue("test1".equals(new String(cols.get(0).getName())));
                        Assert.assertTrue("test2".equals(new String(cols.get(1).getName())));
                    }
                }
        );

        Metrics data = new Metrics();
        data.put("test1", new Metric(Metric.RecordType.AGGREGATE, 1));
        data.put("test2", new Metric(Metric.RecordType.AGGREGATE, 2));

        DateRange range = DateRange.create(Period.MONTHLY, new Date(0));
        ds.persist("testCreate", range.start.getTime(), data);
    }

    @Test
    public void testCreateAbsoluteMetricDoesntAggregate() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(Period.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, Period period) throws IOException
                    {
                        long start = CassandraUtils.unpackLong(predicate.getSlice_range().getStart());
                        if (start > range.start.getTime())
                        {
                            return new ArrayList<SuperColumn>(0);
                        }

                        List<SuperColumn> items = new ArrayList<SuperColumn>(2);
                        List<Column> columns = new ArrayList<Column>(1);

                        columns.add(new Column("test1".getBytes(), SerializerFactory.get().serialize(1), 0));
                        columns.add(new Column("test2".getBytes(), SerializerFactory.get().serialize(2), 0));
                        items.add(new SuperColumn(CassandraUtils.packLong(start), columns));

                        return items;
                    }

                    @Override
                    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException { return null; }

                    @Override
                    public List<Column> getMeta(String entityId) throws IOException
                    {
                        List<Column> columns = new ArrayList<Column>();
                        columns.add(new Column("test1".getBytes("UTF-8"), "absolute".getBytes("UTF-8"), 0));
                        columns.add(new Column("test2".getBytes("UTF-8"), "aggregate".getBytes("UTF-8"), 0));
                        return columns;
                    }

                    @Override
                    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> inserts) throws IOException
                    {
                        Assert.assertEquals("testCreateAbsoluteMetricDoesntAggregate", entityId);
                        Assert.assertTrue(Configuration.get().getSupportedPeriods().size() > 0);

                        for (Period t : Configuration.get().getSupportedPeriods())
                        {
                            Assert.assertTrue(inserts.containsKey(t.toString()));
                        }

                        String t = Configuration.get().getSupportedPeriods().get(0).toString();

                        Assert.assertTrue(inserts.get(t).size() == 1);

                        List<Column> cols = inserts.get(t).get(0).getSuper_column().getColumns();
                        Assert.assertTrue(cols.size() == 3);
                        Assert.assertTrue("test1".equals(new String(cols.get(0).getName())));
                        Assert.assertTrue("test2".equals(new String(cols.get(1).getName())));
                        Assert.assertTrue("test3".equals(new String(cols.get(2).getName())));

                        Assert.assertEquals(1, SerializerFactory.get().deserialize(cols.get(0).getValue()));
                        Assert.assertEquals(4, SerializerFactory.get().deserialize(cols.get(1).getValue()));
                        Assert.assertEquals(3, SerializerFactory.get().deserialize(cols.get(2).getValue()));
                    }
                }
        );

        Metrics data = new Metrics();
        data.put("test1", new Metric(Metric.RecordType.ABSOLUTE, 1));
        data.put("test2", new Metric(Metric.RecordType.AGGREGATE, 2));
        data.put("test3", new Metric(Metric.RecordType.AGGREGATE, 3));

        ds.persist("testCreateAbsoluteMetricDoesntAggregate", range.start.getTime(), data);
    }


    @Test
    public void testCreateSummaryActuallyUpdatesTheSummaryIfItAlreadyExists() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(Period.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, Period period) throws IOException
                    {
                        long start = CassandraUtils.unpackLong(predicate.getSlice_range().getStart());
                        if (start > range.start.getTime())
                        {
                            return new ArrayList<SuperColumn>(0);
                        }

                        List<SuperColumn> items = new ArrayList<SuperColumn>(2);
                        List<Column> columns = new ArrayList<Column>(1);

                        columns.add(new Column("test1".getBytes(), SerializerFactory.get().serialize(1), 0));
                        columns.add(new Column("test2".getBytes(), SerializerFactory.get().serialize(2), 0));
                        items.add(new SuperColumn(CassandraUtils.packLong(start), columns));

                        return items;
                    }

                    @Override
                    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException { return null; }

                    @Override
                    public List<Column> getMeta(String entityId) throws IOException { return null; }

                    @Override
                    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> inserts) throws IOException
                    {
                        Assert.assertEquals("testCreateSummaryActuallyUpdatesTheSummaryIfItAlreadyExists", entityId);
                        Assert.assertTrue(Configuration.get().getSupportedPeriods().size() > 0);

                        for (Period t : Configuration.get().getSupportedPeriods())
                        {
                            Assert.assertTrue(inserts.containsKey(t.toString()));
                        }

                        String t = Configuration.get().getSupportedPeriods().get(0).toString();

                        Assert.assertTrue(inserts.get(t).size() == 1);

                        List<Column> cols = inserts.get(t).get(0).getSuper_column().getColumns();
                        Assert.assertTrue(cols.size() == 3);
                        Assert.assertTrue("test1".equals(new String(cols.get(0).getName())));
                        Assert.assertTrue("test2".equals(new String(cols.get(1).getName())));
                        Assert.assertTrue("test3".equals(new String(cols.get(2).getName())));
                        
                        Assert.assertEquals(2, SerializerFactory.get().deserialize(cols.get(0).getValue()));
                        Assert.assertEquals(4, SerializerFactory.get().deserialize(cols.get(1).getValue()));
                        Assert.assertEquals(3, SerializerFactory.get().deserialize(cols.get(2).getValue()));
                    }
                }
        );

        Metrics data = new Metrics();
        data.put("test1", new Metric(Metric.RecordType.AGGREGATE, 1));
        data.put("test2", new Metric(Metric.RecordType.AGGREGATE, 2));
        data.put("test3", new Metric(Metric.RecordType.AGGREGATE, 3));

        ds.persist("testCreateSummaryActuallyUpdatesTheSummaryIfItAlreadyExists", range.start.getTime(), data);
    }

    @Test
    public void testAggregatingAbsoluteKey() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(Period.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, Period period) throws IOException
                    {
                        long start = CassandraUtils.unpackLong(predicate.getSlice_range().getStart());
                        if (start > range.start.getTime())
                        {
                            return new ArrayList<SuperColumn>(0);
                        }

                        List<SuperColumn> items = new ArrayList<SuperColumn>(2);
                        List<Column> columns = new ArrayList<Column>(1);

                        columns.add(new Column("test1".getBytes(), SerializerFactory.get().serialize(1), 0));
                        columns.add(new Column("test2".getBytes(), SerializerFactory.get().serialize(2), 0));
                        
                        items.add(new SuperColumn(CassandraUtils.packLong(range.start.getTime()), columns));

                        return items;
                    }

                    @Override
                    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException { return null; }

                    @Override
                    public List<Column> getMeta(String entityId) throws IOException
                    {
                        List<Column> columns = new ArrayList<Column>();
                        columns.add(new Column("test1".getBytes("UTF-8"), "absolute".getBytes("UTF-8"), 0));
                        columns.add(new Column("test2".getBytes("UTF-8"), "absolute".getBytes("UTF-8"), 0));
                        return columns;
                    }

                    @Override
                    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Metrics> iter1 = ds.find("view.uid", Period.MONTHLY, new Date(0));
        Iterator<Metrics> iter2 = ds.find("view.uid", Period.MONTHLY, new Date(0));

        Metrics metrics = Metrics.summarize(iter1, iter2);
        Assert.assertEquals(1, metrics.get("test1").getValue());
        Assert.assertEquals(2, metrics.get("test2").getValue());
        Assert.assertEquals(Metric.RecordType.ABSOLUTE, metrics.get("test1").getType());
        Assert.assertEquals(Metric.RecordType.ABSOLUTE, metrics.get("test2").getType());
    }

    @Test
    public void testPartAggregatePartAbsolute() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(Period.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, Period period) throws IOException
                    {
                        long start = CassandraUtils.unpackLong(predicate.getSlice_range().getStart());
                        if (start > range.start.getTime())
                        {
                            return new ArrayList<SuperColumn>(0);
                        }

                        List<SuperColumn> items = new ArrayList<SuperColumn>(2);
                        List<Column> columns = new ArrayList<Column>(1);

                        columns.add(new Column("test1".getBytes(), SerializerFactory.get().serialize(1), 0));
                        columns.add(new Column("test2".getBytes(), SerializerFactory.get().serialize(2), 0));

                        items.add(new SuperColumn(CassandraUtils.packLong(range.start.getTime()), columns));

                        return items;
                    }

                    @Override
                    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException { return null; }

                    @Override
                    public List<Column> getMeta(String entityId) throws IOException
                    {
                        List<Column> columns = new ArrayList<Column>();
                        columns.add(new Column("test1".getBytes("UTF-8"), "absolute".getBytes("UTF-8"), 0));
                        return columns;
                    }

                    @Override
                    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Metrics> iter1 = ds.find("view.uid", Period.MONTHLY, new Date(0));
        Iterator<Metrics> iter2 = ds.find("view.uid", Period.MONTHLY, new Date(0));

        Metrics metrics = Metrics.summarize(iter1, iter2);
        Assert.assertEquals(1, metrics.get("test1").getValue());
        Assert.assertEquals(4, metrics.get("test2").getValue());
        Assert.assertEquals(Metric.RecordType.ABSOLUTE, metrics.get("test1").getType());
        Assert.assertEquals(Metric.RecordType.AGGREGATE, metrics.get("test2").getType());
    }

    @Test
    public void testAggregatingAbsoluteKeyUsesLastItem() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(Period.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    int call = 0;
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, Period period) throws IOException
                    {
                        call += 1;

                        long start = CassandraUtils.unpackLong(predicate.getSlice_range().getStart());
                        if (start > range.start.getTime())
                        {
                            return new ArrayList<SuperColumn>(0);
                        }

                        List<SuperColumn> items = new ArrayList<SuperColumn>(2);
                        List<Column> columns = new ArrayList<Column>(1);

                        if (call == 1)
                        {
                            columns.add(new Column("test1".getBytes(), SerializerFactory.get().serialize(1), 0));
                            columns.add(new Column("test2".getBytes(), SerializerFactory.get().serialize(2), 0));   
                        }
                        else
                        {
                            columns.add(new Column("test1".getBytes(), SerializerFactory.get().serialize(51), 1));
                            columns.add(new Column("test2".getBytes(), SerializerFactory.get().serialize(52), 1));
                        }


                        items.add(new SuperColumn(CassandraUtils.packLong(range.start.getTime()), columns));

                        return items;
                    }

                    @Override
                    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException { return null; }

                    @Override
                    public List<Column> getMeta(String entityId) throws IOException
                    {
                        List<Column> columns = new ArrayList<Column>();
                        columns.add(new Column("test1".getBytes("UTF-8"), "absolute".getBytes("UTF-8"), 0));
                        columns.add(new Column("test2".getBytes("UTF-8"), "absolute".getBytes("UTF-8"), 0));
                        return columns;
                    }

                    @Override
                    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> superColumnOperations) throws IOException {}
                }
        );

        Iterator<Metrics> iter1 = ds.find("view.uid", Period.MONTHLY, new Date(0));
        Iterator<Metrics> iter2 = ds.find("view.uid", Period.MONTHLY, new Date(0));

        Metrics metrics = Metrics.summarize(iter1, iter2);
        Assert.assertEquals(51, metrics.get("test1").getValue());
        Assert.assertEquals(52, metrics.get("test2").getValue());
        
        Assert.assertEquals(Metric.RecordType.ABSOLUTE, metrics.get("test1").getType());
        Assert.assertEquals(Metric.RecordType.ABSOLUTE, metrics.get("test2").getType());
    }

    @Test
    public void testPersisIncludesMetaUpdate() throws Exception
    {
        DataStore ds = get();

        final DateRange range = DateRange.create(Period.MONTHLY, new Date(0));

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    int call = 0;
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, Period period) throws IOException
                    {
                        return new ArrayList<SuperColumn>(0);
                    }

                    @Override
                    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException { return null; }

                    @Override
                    public List<Column> getMeta(String entityId) throws IOException { return null; }

                    @Override
                    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> superColumnOperations) throws IOException
                    {
                        Assert.assertTrue(superColumnOperations.containsKey("meta"));
                        Assert.assertTrue(superColumnOperations.get("meta").size() == 1);
                        Assert.assertFalse(superColumnOperations.get("meta").get(0).isSetSuper_column());
                        Assert.assertTrue(superColumnOperations.get("meta").get(0).isSetColumn());
                        Assert.assertEquals("jessica.robinson".getBytes(), superColumnOperations.get("meta").get(0).getColumn().getName());
                    }
                }
        );

        Metrics metrics = new Metrics();
        metrics.put("jessica.robinson", new Metric(Metric.RecordType.ABSOLUTE, 100));
    }

    @Test(expected=java.io.IOException.class)
    public void testLockingAKeyWontWriteIt() throws Exception
    {
        DataStore ds = get();

        CassandraQueryFactory.setTestMock(
                new CassandraQuery() {
                    @Override
                    public List<SuperColumn> find(String entityId, SlicePredicate predicate, Period period) throws IOException
                    {
                        return new ArrayList<SuperColumn>(0);
                    }

                    @Override
                    public List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException { return null; }
                    
                    @Override
                    public List<Column> getMeta(String entityId) throws IOException { return null; }

                    @Override
                    public void persist(String entityId, Map<String, List<ColumnOrSuperColumn>> superColumnOperations) throws IOException
                    {
                        Assert.fail("Shouldn't persist an item if I'm unable to get a lock.");
                    }
                }
        );

        Lock lock = LockFactory.get();
        lock.acquire("123");

        try
        {
            Metrics data = new Metrics();
            data.put("test1", new Metric(Metric.RecordType.AGGREGATE, 1));

            ds.persist("123", new Date(0).getTime(), data);
        }
        finally
        {
            lock.release("123");
        }
    }
}
