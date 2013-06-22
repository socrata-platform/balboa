package com.socrata.metrics.migrate

import org.junit.Test
import com.socrata.metrics.{DomainId, ViewUid}

/**
 *
 */
class MetricRecordTest {
  @Test def testRecordViews {
    val touched = new MetricRecord().recordViews(ViewUid("four-by-four"), DomainId(3))
    touched.foreach {
      m => println(m)
    }
  }
}