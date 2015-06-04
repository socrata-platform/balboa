package com.socrata.balboa.metrics.data.impl
import java.util.Date

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.data.Period
import com.socrata.balboa.metrics.{Metric, Metrics}
import org.junit.Test

/**
 *
 */
class BadIdeasDataStoreTest {
  val badIdeas = new BadIdeasDataStore(new MockCassandraDataStore)

  @Test(expected = classOf[IllegalArgumentException])
  def testCantPersistUnderscoredColumns {
    val metrics = new Metrics()
    metrics.put("__taco__", new Metric(RecordType.AGGREGATE, 4))
    badIdeas.persist("pizza", 0, metrics)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testCantPersistUnderscoredEntities {
      badIdeas.persist("__pizza__", 0, new Metrics())
  }

}

class MockDataStore extends DataStoreImpl {
  def entities(pattern: String) = null
  def entities() = null
  def slices(entityId: String, period: Period, start: Date, end: Date) = null
  def find(entityId: String, period: Period, date: Date) = null
  def find(entityId: String, period: Period, start: Date, end: Date) = null
  def find(entityId: String, start: Date, end: Date) = null
  def persist(entityId: String, timestamp: Long, metrics: Metrics) {}
}
