package com.socrata.balboa.metrics.data.impl;

import com.socrata.balboa.metrics.data.DateRange;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SuperColumn;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface CassandraQuery
{
    List<KeySlice> getKeys(String columnFamily, KeyRange range) throws IOException;
    SuperColumn getMeta(String entityId) throws IOException;
    List<SuperColumn> find(String entityId, SlicePredicate predicate, DateRange.Period period) throws IOException;
    void persist(String entityId, Map<String, List<SuperColumn>> superColumnOperations) throws IOException;
}
