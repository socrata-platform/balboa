package com.socrata.balboa.metrics.data.impl

/**
 * Mock Data Store using the Mock Cassandra Driver [[MockCassandra11QueryImpl]].
 */
class MockCassandraDataStore extends Cassandra11DataStore(new MockCassandra11QueryImpl)
