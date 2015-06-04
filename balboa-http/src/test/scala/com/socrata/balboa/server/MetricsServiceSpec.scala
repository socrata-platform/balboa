package com.socrata.balboa.server

import java.util.Date

import com.socrata.balboa.metrics.data.{Period, DataStore}
import com.socrata.balboa.metrics.data.impl.MockCassandraDataStore
import com.socrata.balboa.server.MetricsServiceSpecSetup.{SimpleMockDataStore, EmptyMockDataStore, TestData, MockDataStoreBase}
import com.socrata.balboa.util.MetricsTestStuff.{TestEntityIDs, TestMetrics, TestTimeStamps}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, WordSpec}

object MetricsServiceSpecSetup extends MockitoSugar {

  /**
   * Reusable Mock Kafka Stream
   */
  trait MockDataStoreBase {
    implicit val ds: DataStore = new MockCassandraDataStore
  }

  /**
   * Consolidated Trait for Test Data.
   */
  trait TestData extends TestMetrics with TestTimeStamps with TestEntityIDs

  /**
   * Initially empty
   */
  trait EmptyMockDataStore extends MockDataStoreBase with TestData

  /**
   * Data Store where each entry has a single metric with value one
   */
  trait SimpleMockDataStore extends MockDataStoreBase with TestData {
    allTimeStamps.foreach(d => ds.persist(balboa, d.getTime, oneElemMetrics))
  }

  /**
   * Data Store where each entry has multiple metrics.
   */
  trait FullMockDataStore extends MockDataStoreBase with TestData {
    allTimeStamps.foreach(d => ds.persist(balboa, d.getTime, manyElemMetrics))
  }

}

/**
 * Unit Test for the [[MetricsService]].
 */
class MetricsServiceSpec extends WordSpec with BeforeAndAfterEach {

  "A MetricsService " when {

    "calculating a period" should {

      "fail when Entity ID is null" in new EmptyMockDataStore {
        assert(MetricsService.period(null, new Date(), Period.FOREVER, None, None).isFailure)
      }

      "fail when Date is null" in new EmptyMockDataStore {
        assert(MetricsService.period(balboa, null, Period.FOREVER, None, None).isFailure)
      }

      "fail when Period is null" in new EmptyMockDataStore {
        assert(MetricsService.period(balboa, new Date(), null, None, None).isFailure)
      }

      "fail when the optional combination filter is null" in new EmptyMockDataStore {
        assert(MetricsService.period(balboa, new Date(), Period.FOREVER, null, None).isFailure)
      }

      "fail when the optional metric filter is null" in new EmptyMockDataStore {
        assert(MetricsService.period(balboa, new Date(), Period.FOREVER, None, null).isFailure)
      }

      "return no metrics for a period where there is no data" in new EmptyMockDataStore {
//        val res = MetricsService.period("entityID", new Date(), Period.HOURLY, None, None)
//        assert(res.isSuccess)
//        assert(res.get.isEmpty)
      }

      "return the aggregation of metrics within the a specific Time Window" in new SimpleMockDataStore {

      }

    }

    "calculating a series" when {

      "" should  {

      }

    }

    "calculating a range" when {

      "" should  {

      }

    }



  }

}
