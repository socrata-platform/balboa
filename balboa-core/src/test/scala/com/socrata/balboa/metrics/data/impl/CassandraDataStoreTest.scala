package com.socrata.balboa.metrics.data.impl

import java.util.concurrent.TimeUnit
import java.util.{Date, GregorianCalendar, TimeZone}

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.metrics.data.{DateRange, Period}
import com.socrata.balboa.metrics.{Metric, Metrics}
import junit.framework.Assert
import org.junit.{Before, Ignore, Test}

import scala.collection.JavaConverters._

class CassandraDataStoreTest {
  val mock = new MockCassandraQueryImpl()
  val cds: CassandraDataStore = new CassandraDataStore(mock)
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
  def setUp(): Unit = {
    testMetrics.put(aggMetricName, aggMetric)
    testMetrics.put(absMetricName, absMetric)
  }

  @Test
  @Ignore("Requires a local cassandra server and should be executed in isolation")
  def testPersistIntegration(): Unit = {
    val cds: CassandraDataStore = new CassandraDataStore()
    cds.persist(testEntity, System.currentTimeMillis(), testMetrics)
  }

  @Test
  def testPersist(): Unit = {
    cds.persist(testEntity, 12345, testMetrics)
    Assert.assertEquals(getPersistExpect(12345), mock.persists)
  }

  @Test
  def testFindSingleDateWithinTier(): Unit = {
    mock.metricsToReturn = new Metrics(Map(aggMetricName -> aggMetric).asJava)
    Assert.assertEquals(mock.metricsToReturn, cds.find(testEntity, Period.HOURLY, new Date(12345L)).next())
    Assert.assertEquals(List(new AFetch(testEntity + "-" + 0, Period.HOURLY)), mock.fetches)
  }

  @Test
  def testFindSingleDateWithinUnsupportedTier(): Unit = {
    mock.metricsToReturn = new Metrics(Map(aggMetricName -> aggMetric).asJava)
    mock.fetches = List()
    val fItr = cds.find(testEntity, Period.YEARLY, new Date(1351236163000L))
    for (i <- 1 to 12) {
      Assert.assertEquals(mock.metricsToReturn, fItr.next)
    }
    Assert.assertEquals(List( new AFetch(testEntity + "-1325376000000", Period.MONTHLY),
                              new AFetch(testEntity + "-1328054400000", Period.MONTHLY),
                              new AFetch(testEntity + "-1330560000000", Period.MONTHLY),
                              new AFetch(testEntity + "-1333238400000", Period.MONTHLY),
                              new AFetch(testEntity + "-1335830400000", Period.MONTHLY),
                              new AFetch(testEntity + "-1338508800000", Period.MONTHLY),
                              new AFetch(testEntity + "-1341100800000", Period.MONTHLY),
                              new AFetch(testEntity + "-1343779200000", Period.MONTHLY),
                              new AFetch(testEntity + "-1346457600000", Period.MONTHLY),
                              new AFetch(testEntity + "-1349049600000", Period.MONTHLY),
                              new AFetch(testEntity + "-1351728000000", Period.MONTHLY),
                              new AFetch(testEntity + "-1354320000000", Period.MONTHLY)), mock.fetches)
}

  @Test
  def testFindRangeWithinTier(): Unit = {
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
  def testFindOptimized(): Unit = {
    mock.metricsToReturn = new Metrics(Map(aggMetricName -> aggMetric).asJava)
    val start = new Date(TimeUnit.MILLISECONDS.convert(28, TimeUnit.DAYS) + TimeUnit.MILLISECONDS.convert(22, TimeUnit.HOURS)) // two hours before midnight, Jan 30 1970
    val end = new Date(TimeUnit.MILLISECONDS.convert(60, TimeUnit.DAYS) + TimeUnit.MILLISECONDS.convert(25, TimeUnit.HOURS)) // to two hours after midnight, Mar 3 1970
    val itr = cds.find(testEntity, start, end)
    while(itr.hasNext)
      Assert.assertEquals(mock.metricsToReturn, itr.next())
    Assert.assertEquals(List(new AFetch("foo-2498400000", Period.HOURLY),
                             new AFetch("foo-2502000000", Period.HOURLY),
                             new AFetch("foo-2505600000", Period.DAILY),
                             new AFetch("foo-2592000000", Period.DAILY),
                             new AFetch("foo-2678400000", Period.MONTHLY),
                             new AFetch("foo-5097600000", Period.DAILY),
                             new AFetch("foo-5184000000", Period.DAILY),
                             new AFetch("foo-5270400000", Period.HOURLY),
                             new AFetch("foo-5274000000", Period.HOURLY)),  mock.fetches)
  }

  @Test
  def testGetSlices(): Unit = {
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
  def testGetUnSupportedSlices(): Unit = {
    mock.metricsToReturn = new Metrics(Map(aggMetricName -> aggMetric).asJava)
    val start = new Date(TimeUnit.MILLISECONDS.convert(28, TimeUnit.DAYS) + TimeUnit.MILLISECONDS.convert(22, TimeUnit.HOURS)) // two hours before midnight, Jan 30 1970
    val end = new Date(TimeUnit.MILLISECONDS.convert(60, TimeUnit.DAYS) + TimeUnit.MILLISECONDS.convert(25, TimeUnit.HOURS)) // to two hours after midnight, Mar 3 1970

    // Should be converted to a daily query because weekly is not supported
    val timeSliceItr = cds.slices(testEntity, Period.WEEKLY, start, end)

    var count = 0
    while (timeSliceItr.hasNext) {
      val ts = timeSliceItr.next()
      Assert.assertTrue(DateRange.liesOnBoundary(new Date(ts.getStart), Period.WEEKLY))
      count += 1
    }

    // 6 full weeks between Jan 30, 1970 and Mar, 3 - Sun -> Sat
    Assert.assertEquals(6, count)

    // Check that we actually fetched from a supported bin
    val times = List.range(2419200000L, 5270400001L, 86400000)
    val fs = times.map(t => new AFetch("foo-" + t, Period.DAILY))
    Assert.assertEquals(fs, mock.fetches)
  }

  @Test
  def testGetEntities(): Unit = {
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
  def testGetEntitiesPattern(): Unit = {
    val entyItr = cds.entities("t") // only entities with a 't' in them
    var count = 0
    while(entyItr.hasNext) {
      count += 1
      val enty = entyItr.next()
      Assert.assertFalse(enty.contains("-"))
    }
    Assert.assertEquals(2, count)
  }

  @Test def testRangeScanNoItems(): Unit = {
    mock.metricsToReturn = null
    val start = new Date(TimeUnit.MILLISECONDS.convert(28, TimeUnit.DAYS) + TimeUnit.MILLISECONDS.convert(22, TimeUnit.HOURS)) // two hours before midnight, Jan 30 1970
    val end = new Date(TimeUnit.MILLISECONDS.convert(60, TimeUnit.DAYS) + TimeUnit.MILLISECONDS.convert(25, TimeUnit.HOURS)) // to two hours after midnight, Mar 3 1970
    val itr = cds.find(testEntity, start, end)

    // Nothing found
    Assert.assertFalse(itr.hasNext)

    // Should still try fetching the same data
    Assert.assertEquals(List(new AFetch("foo-2498400000", Period.HOURLY),
      new AFetch("foo-2502000000", Period.HOURLY),
      new AFetch("foo-2505600000", Period.DAILY),
      new AFetch("foo-2592000000", Period.DAILY),
      new AFetch("foo-2678400000", Period.MONTHLY),
      new AFetch("foo-5097600000", Period.DAILY),
      new AFetch("foo-5184000000", Period.DAILY),
      new AFetch("foo-5270400000", Period.HOURLY),
      new AFetch("foo-5274000000", Period.HOURLY)),  mock.fetches)
  }

  def rollUpIteratorTest(requestPeriod:Period, supportedPeriod:Period, startMS:Long, endMS:Long) {
    mock.metricsToReturn = new Metrics(Map(aggMetricName -> aggMetric).asJava)
    val startPeriod = DateRange.create(requestPeriod, new Date(startMS)) // start from the reference point
    val endPeriod = DateRange.create(requestPeriod, new Date(endMS))
    val dr = new DateRange(startPeriod.start, endPeriod.end)
    val inRanges = dr.toDates(supportedPeriod).asScala.toList; // requesting as the supported period
    val expectedRanges = dr.toDates(requestPeriod).asScala.toList

    // Run it through the roll-up iterator
    val tsItr = CassandraUtil.sliceIterator(mock, "blah", supportedPeriod, inRanges)
    val rdItr = CassandraUtil.rollupSliceIterator(requestPeriod, tsItr)

    // Pretend we support this period
    val expectedItr = CassandraUtil.sliceIterator(mock, "blah", requestPeriod, expectedRanges)

    // We can have more requested timeslices returned than if the query were natively supported
    // because
    while(rdItr.hasNext && expectedItr.hasNext) {
      val ts = rdItr.next()
      val expect = expectedItr.next()
      requestPeriod match {
        case Period.MONTHLY => {}
        case _ =>
          Assert.assertEquals("Expected start to be == rolledUp start",  expect.getStart, ts.getStart) // time slices do not have to exactly align on boundarys
          Assert.assertEquals("Expected end to be == rolledUp end",expect.getEnd, ts.getEnd)
      }

      Assert.assertTrue(DateRange.liesOnBoundary(new Date(ts.getStart), requestPeriod))
    }
  }

  @Test
  def rollUpSliceToLessGranularPeriod() {
    rollUpIteratorTest(Period.WEEKLY, Period.DAILY, 1000000, 1000000 + 604800000 * 3)
    rollUpIteratorTest(Period.DAILY, Period.MINUTELY, 1000000, 1000000 + 86400000 * 6)
    rollUpIteratorTest(Period.MONTHLY, Period.DAILY, 1351228407, 1351228407 + 2628000000L * 2)
  }

  @Test
  def rollUpSliceHandlesDatesInThePast() {
    rollUpIteratorTest(Period.WEEKLY, Period.DAILY, -1000000, -1000000 + 604800000 * 3)
    rollUpIteratorTest(Period.DAILY, Period.MINUTELY, -1351228407, -1351228407 + 86400000 * 6)
  }

  @Test
  def rollUpSliceCrossOddBoundary() {
    val start = new GregorianCalendar(TimeZone.getTimeZone("UTC"))
    start.set(2010, 0, 12)
    val end = new GregorianCalendar(TimeZone.getTimeZone("UTC"))
    end.set(2010, 3, 16)
    rollUpIteratorTest(Period.MONTHLY, Period.WEEKLY, start.getTime.getTime, end.getTime.getTime)
  }

  @Test
  def testGetValidGranularity() {
    Assert.assertEquals(Period.DAILY, cds.getValidGranularity(Period.WEEKLY))
    Assert.assertEquals(Period.MONTHLY, cds.getValidGranularity(Period.YEARLY))
    Assert.assertEquals(Period.HOURLY, cds.getValidGranularity(Period.SECONDLY))
  }
}
