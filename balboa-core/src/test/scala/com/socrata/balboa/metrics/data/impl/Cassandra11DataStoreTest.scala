package com.socrata.balboa.metrics.data.impl

import com.socrata.balboa.metrics.{Metric, Metrics}
import org.junit.{Before, Test, Ignore}
import junit.framework.Assert
import com.socrata.balboa.metrics.data.{DateRange, Period}
import java.util.Date
import scala.collection.JavaConverters._
import java.util.concurrent.TimeUnit
import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.config.Configuration

/**
 *
 */
class Cassandra11DataStoreTest {

  val mock = new MockCassandra11QueryImpl()
  val cds: Cassandra11DataStore = new Cassandra11DataStore(mock)
  val testMetrics: Metrics = new Metrics()
  val testEntity = "foo"
  val aggMetric = new Metric(Metric.RecordType.AGGREGATE, 1)
  val absMetric = new Metric(Metric.RecordType.ABSOLUTE, 777)
  val aggMetricName = "myfoometric"
  val absMetricName = "oatmeals"

  def getPersistExpect(ts:Long) =
    List[APersist](
      new APersist(testEntity + "-" + DateRange.create(Period.MONTHLY, new Date(ts)).start.getTime, Period.MONTHLY, Map(aggMetricName -> aggMetric), Map(absMetricName -> absMetric)),
      new APersist(testEntity + "-" + DateRange.create(Period.DAILY, new Date(ts)).start.getTime, Period.DAILY, Map(aggMetricName -> aggMetric),  Map(absMetricName -> absMetric)),
      new APersist(testEntity + "-" + DateRange.create(Period.HOURLY, new Date(ts)).start.getTime, Period.HOURLY, Map(aggMetricName -> aggMetric), Map(absMetricName -> absMetric)))

  @Before
  def setUp {
    testMetrics.put(aggMetricName, aggMetric)
    testMetrics.put(absMetricName, absMetric)
  }

  @Test
  @Ignore("Requires a local cassandra server and should be executed in isolation")
  def testPersistIntegration {
    val cds: Cassandra11DataStore = new Cassandra11DataStore()
    cds.persist(testEntity, System.currentTimeMillis(), testMetrics)
  }



  @Test
  def testPersist {
    cds.persist(testEntity, 12345, testMetrics)
    Assert.assertEquals(getPersistExpect(12345), mock.persists)
  }

  @Test
  def testFindSingleDateWithinTier {
    cds.persist(testEntity, 12345, testMetrics)
    mock.metricsToReturn = new Metrics(Map(aggMetricName -> aggMetric).asJava)
    Assert.assertEquals(mock.metricsToReturn, cds.find(testEntity, Period.HOURLY, new Date(12345)).next())
    Assert.assertEquals(List(new AFetch(testEntity + "-" + 0, Period.HOURLY)), mock.fetches)
  }

  @Test
  def testFindRangeWithinTier {
    mock.metricsToReturn = new Metrics(Map(aggMetricName -> aggMetric).asJava)
    val itr = cds.find(testEntity, Period.HOURLY, new Date(0), new Date(TimeUnit.MILLISECONDS.convert(3, TimeUnit.HOURS)))
    Assert.assertEquals(mock.metricsToReturn, itr.next())
    Assert.assertEquals(mock.metricsToReturn, itr.next())
    Assert.assertEquals(mock.metricsToReturn, itr.next())
    Assert.assertFalse(itr.hasNext)
    Assert.assertEquals(List( new AFetch(testEntity + "-" + TimeUnit.MILLISECONDS.convert(0, TimeUnit.HOURS), Period.HOURLY),
                              new AFetch(testEntity + "-" + TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS), Period.HOURLY),
                              new AFetch(testEntity + "-" + TimeUnit.MILLISECONDS.convert(2, TimeUnit.HOURS), Period.HOURLY)), mock.fetches)

  }

  @Test
  def testFindOptimized {
    mock.metricsToReturn = new Metrics(Map(aggMetricName -> aggMetric).asJava)
    val start = new Date(TimeUnit.MILLISECONDS.convert(28, TimeUnit.DAYS) + TimeUnit.MILLISECONDS.convert(22, TimeUnit.HOURS)) // two hours before midnight, Jan 30 1970
    val end = new Date(TimeUnit.MILLISECONDS.convert(60, TimeUnit.DAYS) + TimeUnit.MILLISECONDS.convert(25, TimeUnit.HOURS)) // to two hours after midnight, Mar 3 1970
    val itr = cds.find(testEntity, start, end)
    while(itr.hasNext)
      Assert.assertEquals(mock.metricsToReturn, itr.next())
    Assert.assertEquals(List(new AFetch("foo-2498400000", Period.HOURLY),
                             new AFetch("foo-2502000000", Period.HOURLY),
                             new AFetch("foo-5270400000", Period.HOURLY),
                             new AFetch("foo-5274000000", Period.HOURLY),
                             new AFetch("foo-2505600000", Period.DAILY),
                             new AFetch("foo-2592000000", Period.DAILY),
                             new AFetch("foo-5097600000", Period.DAILY),
                             new AFetch("foo-5184000000", Period.DAILY),
                             new AFetch("foo-2678400000", Period.MONTHLY)).sorted,  mock.fetches.sorted)
  }

  @Test
  def testGetSlices {
    mock.metricsToReturn = new Metrics(Map(aggMetricName -> aggMetric).asJava)
    val start = new Date(TimeUnit.MILLISECONDS.convert(28, TimeUnit.DAYS) + TimeUnit.MILLISECONDS.convert(22, TimeUnit.HOURS)) // two hours before midnight, Jan 30 1970
    val end = new Date(TimeUnit.MILLISECONDS.convert(60, TimeUnit.DAYS) + TimeUnit.MILLISECONDS.convert(25, TimeUnit.HOURS)) // to two hours after midnight, Mar 3 1970

    var timeSliceItr = cds.slices(testEntity, Period.MONTHLY, start, end)

    // Months are so large that we create three slices for them; one for each month, Jan, Feb, March
    Assert.assertTrue(timeSliceItr.hasNext)
    Assert.assertEquals(mock.metricsToReturn, timeSliceItr.next().getMetrics)
    Assert.assertTrue(timeSliceItr.hasNext)
    Assert.assertEquals(mock.metricsToReturn, timeSliceItr.next().getMetrics)
    Assert.assertTrue(timeSliceItr.hasNext)
    Assert.assertEquals(mock.metricsToReturn, timeSliceItr.next().getMetrics)
    Assert.assertFalse(timeSliceItr.hasNext)

    timeSliceItr = cds.slices(testEntity, Period.HOURLY, start, end)
    mock.fetches = List() // clear fetch list so we can count properly
    var count = 0
    while(timeSliceItr.hasNext) {
      count += 1
      Assert.assertEquals(mock.metricsToReturn, timeSliceItr.next().getMetrics)
      Assert.assertEquals(count, mock.fetches.size) // check that the iterator actually iterates
    }
    Assert.assertEquals(771, count) // yup.
  }

  @Test
  def testGetEntities {
    val entyItr = cds.entities()
    var count = 0
    while(entyItr.hasNext) {
      count += 1
      val enty = entyItr.next()
      Assert.assertFalse(enty.contains("-"))
    }
    Assert.assertEquals(mock.uniqEntityNames, count)
    Assert.assertEquals(2, mock.entities.size)
    Assert.assertEquals(RecordType.AGGREGATE, mock.entities(0).recordType)
    Assert.assertEquals(RecordType.ABSOLUTE, mock.entities(1).recordType)
    Assert.assertEquals(Period.MONTHLY, mock.entities(0).period)
    Assert.assertEquals(Period.MONTHLY, mock.entities(1).period)
  }

  @Test
  def testGetEntitiesPattern {
    val entyItr = cds.entities("t") // only entities with a 't' in them
    var count = 0
    while(entyItr.hasNext) {
      count += 1
      val enty = entyItr.next()
      Assert.assertFalse(enty.contains("-"))
    }
    Assert.assertEquals(2, count)
  }

  @Test def testRangeScanNoItems {
    mock.metricsToReturn = null
    val start = new Date(TimeUnit.MILLISECONDS.convert(28, TimeUnit.DAYS) + TimeUnit.MILLISECONDS.convert(22, TimeUnit.HOURS)) // two hours before midnight, Jan 30 1970
    val end = new Date(TimeUnit.MILLISECONDS.convert(60, TimeUnit.DAYS) + TimeUnit.MILLISECONDS.convert(25, TimeUnit.HOURS)) // to two hours after midnight, Mar 3 1970
    val itr = cds.find(testEntity, start, end)

    // Nothing found
    Assert.assertFalse(itr.hasNext)

    // Should still try fetching the same data
    Assert.assertEquals(List(new AFetch("foo-2498400000", Period.HOURLY),
      new AFetch("foo-2502000000", Period.HOURLY),
      new AFetch("foo-5270400000", Period.HOURLY),
      new AFetch("foo-5274000000", Period.HOURLY),
      new AFetch("foo-2505600000", Period.DAILY),
      new AFetch("foo-2592000000", Period.DAILY),
      new AFetch("foo-5097600000", Period.DAILY),
      new AFetch("foo-5184000000", Period.DAILY),
      new AFetch("foo-2678400000", Period.MONTHLY)).sorted,  mock.fetches.sorted)
  }

}
