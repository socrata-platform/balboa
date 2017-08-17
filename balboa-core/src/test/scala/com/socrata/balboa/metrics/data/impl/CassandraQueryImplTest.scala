package com.socrata.balboa.metrics.data.impl

import java.io.IOException
import java.util.Date

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.data.{BalboaFastFailCheck, DateRange, Period}
import com.socrata.balboa.metrics.{Metric, Metrics}
import com.typesafe.config.{Config, ConfigFactory}
import junit.framework.Assert
import org.junit.{Before, Ignore, ETest}
import org.mockito.Mockito.{spy, when}
import org.scalatest.mock.MockitoSugar

/**
 * Integration Test; Tests Ignored Manually
 */
class CassandraQueryImplTest extends MockitoSugar{
  @Before
  def clearCassandra(): Unit = {
    CassandraUtil.initializeContext().truncateAll()
  }

  @Test
  @Ignore("Requires a local cassandra server and should be executed in isolation")
  def testFetchSet(): Unit = {
    val q:CassandraQuery = new CassandraQueryImpl(CassandraUtil.initializeContext())
    q.persist("mykey", DateRange.create(Period.HOURLY,new Date(1000)).start, Period.HOURLY,
      Map("mymetric1" -> new Metric(RecordType.AGGREGATE, 1), "mymetric2" -> new Metric(RecordType.AGGREGATE, 555)),
      Map("mymetric3" -> new Metric(RecordType.ABSOLUTE, 666), "mymetric4" -> new Metric(RecordType.ABSOLUTE, 777)))
    val m:Metrics = q.fetch("mykey", Period.HOURLY, new DateRange(new Date(0), new Date(3700000)).start)
    Assert.assertFalse(BalboaFastFailCheck.getInstance().isInFailureMode)
    Assert.assertEquals(4, m.size())
  }

  @Test
  @Ignore("Requires a local cassandra server and should be executed in isolation")
  def testFetchSetWithUnicode(): Unit = {
    val q:CassandraQuery = new CassandraQueryImpl(CassandraUtil.initializeContext())
    q.persist("mykey", DateRange.create(Period.HOURLY,new Date(1000)).start, Period.HOURLY,
      Map("Execució" -> new Metric(RecordType.AGGREGATE, 1)),
      Map("Execució Aussi" -> new Metric(RecordType.ABSOLUTE, 1)))
    val m:Metrics = q.fetch("mykey", Period.HOURLY, new DateRange(new Date(0), new Date(2000)).start)
    Assert.assertTrue(m.containsKey("Execució"))
    val metric = new Metric();
    metric.setType(RecordType.ABSOLUTE)
    metric.setValue(1l)
    Assert.assertEquals(metric, m.get("Execució Aussi"))
    Assert.assertEquals(2, m.size())
  }

  @Test
  @Ignore("Requires a local cassandra server and should be executed in isolation")
  def testGetKeys(): Unit = {
    val q:CassandraQuery = new CassandraQueryImpl(CassandraUtil.initializeContext())
    q.persist("mykey", DateRange.create(Period.HOURLY,new Date(1000)).start, Period.HOURLY,
      Map("mymetric1" -> new Metric(RecordType.AGGREGATE, 1), "mymetric2" -> new Metric(RecordType.AGGREGATE, 555)),
      Map("mymetric3" -> new Metric(RecordType.ABSOLUTE, 666), "mymetric4" -> new Metric(RecordType.ABSOLUTE, 777)))
    val keysItr:Iterator[String] =
      new CassandraQueryImpl(CassandraUtil.initializeContext())
        .getAllEntityIds(RecordType.AGGREGATE, Period.HOURLY)
    val keys = keysItr.toSeq
    Assert.assertEquals(1, keys.length)
    Assert.assertEquals("mykey", keys(0))
  }

  @Test(expected = classOf[IOException])
  @Ignore("Requires a local cassandra server and should be executed in isolation")
  def testConnectionFailureOnPersist() {
    BalboaFastFailCheck.getInstance().markSuccess()

    val conf: Config = spy[Config](ConfigFactory.load())
    when(conf.getString("cassandra.servers")).thenReturn("example.test-socrata.com:12345")
    val q:CassandraQuery = new CassandraQueryImpl(CassandraUtil.initializeContext(conf))
    try {
      Assert.assertTrue(BalboaFastFailCheck.getInstance().proceed())
      q.persist("mykey", DateRange.create(Period.HOURLY,new Date(1000)).start, Period.HOURLY,
        Map("mymetric1" -> new Metric(RecordType.AGGREGATE, 1)),
        Map("mymetric3" -> new Metric(RecordType.ABSOLUTE, 666)))
    } finally {
      Assert.assertTrue(BalboaFastFailCheck.getInstance().isInFailureMode)
    }
  }

  @Test(expected = classOf[IOException])
  @Ignore("Requires a local cassandra server and should be executed in isolation")
  def testConnectionFailureOnFetch() {
    BalboaFastFailCheck.getInstance().markSuccess()

    val conf = spy[Config](ConfigFactory.load())
    when(conf.getString("cassandra.servers")).thenReturn("example.test-socrata.com:12345")
    val q:CassandraQuery = new CassandraQueryImpl(CassandraUtil.initializeContext(conf))
    try {
      Assert.assertTrue(BalboaFastFailCheck.getInstance().proceed())
      q.fetch("mykey", Period.HOURLY, new DateRange(new Date(1000), new Date(2000)).start)
    } finally {
      Assert.assertTrue(BalboaFastFailCheck.getInstance().isInFailureMode)
    }
  }

}
