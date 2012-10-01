package com.socrata.balboa.metrics.data.impl

import org.junit.{Ignore, Test}
import com.socrata.balboa.metrics.{Metrics, Metric}
import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.data.{BalboaFastFailCheck, Period, DateRange}
import junit.framework.Assert
import java.util.Date
import com.socrata.balboa.metrics.config.Configuration
import java.io.IOException

/**
 * Integration Test; Tests Ignored Manually
 */
class Cassandra11QueryImplTest {
  @Test
  @Ignore("Requires a local cassandra server and should be executed in isolation")
  def testFetchSet {
    val q:Cassandra11Query = new Cassandra11QueryImpl(Cassandra11Util.initializeContext())
    q.persist("mykey", DateRange.create(Period.HOURLY,new Date(1000)).start, Period.HOURLY,
      Map("mymetric1" -> new Metric(RecordType.AGGREGATE, 1), "mymetric2" -> new Metric(RecordType.AGGREGATE, 555)),
      Map("mymetric3" -> new Metric(RecordType.ABSOLUTE, 666), "mymetric4" -> new Metric(RecordType.ABSOLUTE, 777)))
    val m:Metrics = q.fetch("mykey", Period.HOURLY, new DateRange(new Date(1000), new Date(2000)).start)
    Assert.assertEquals(4, m.size())
  }

  @Test
  @Ignore("Requires a local cassandra server and should be executed in isolation")
  def testGetKeys {
    val keysItr:Iterator[String] = new Cassandra11QueryImpl(Cassandra11Util.initializeContext()).getAllEntityIds(RecordType.AGGREGATE, Period.HOURLY)
    keysItr.foreach(println)
  }

  @Ignore("Requires a local cassandra server and should be executed in isolation")
  @Test(expected = classOf[IOException])
  def testConnectionFailureOnPersist() {
    BalboaFastFailCheck.getInstance().markSuccess()
    Configuration.get().setProperty("cassandra.servers", "example.test-socrata.com:12345")
    val q:Cassandra11Query = new Cassandra11QueryImpl(Cassandra11Util.initializeContext())
    try {
      Assert.assertTrue(BalboaFastFailCheck.getInstance().proceed())
      q.persist("mykey", DateRange.create(Period.HOURLY,new Date(1000)).start, Period.HOURLY,
        Map("mymetric1" -> new Metric(RecordType.AGGREGATE, 1)),
        Map("mymetric3" -> new Metric(RecordType.ABSOLUTE, 666)))
    } finally {
      Assert.assertTrue(BalboaFastFailCheck.getInstance().isInFailureMode)
    }
  }

  @Ignore("Requires a local cassandra server and should be executed in isolation")
  @Test(expected = classOf[IOException])
  def testConnectionFailureOnFetch() {
    BalboaFastFailCheck.getInstance().markSuccess()
    Configuration.get().setProperty("cassandra.servers", "example.test-socrata.com:12345")
    val q:Cassandra11Query = new Cassandra11QueryImpl(Cassandra11Util.initializeContext())
    try {
      Assert.assertTrue(BalboaFastFailCheck.getInstance().proceed())
      q.fetch("mykey", Period.HOURLY, new DateRange(new Date(1000), new Date(2000)).start)
    } finally {
      Assert.assertTrue(BalboaFastFailCheck.getInstance().isInFailureMode)
    }
  }


}