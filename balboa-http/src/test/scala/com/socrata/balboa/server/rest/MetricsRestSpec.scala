package com.socrata.balboa.server.rest

import com.socrata.balboa.metrics.data.DataStore
import org.scalatest.WordSpec
import org.scalatest.mock.EasyMockSugar

object MetricsRestSpecSetup {

  trait MockDataStore extends EasyMockSugar {
    val ds = mock[DataStore]
  }

}

/**
 * Unit Test for Metrics Rest Service.
 */
class MetricsRestSpec extends WordSpec {

  "Metrics REST Service" should {

    ""

  }

}
