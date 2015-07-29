package com.socrata.balboa.metrics.data.impl

import java.util.Date

import com.socrata.balboa.metrics.Metrics
import org.junit.{Assert, Test}

class MemoryDataStoreTest {

  @Test
  def persistAndFind(): Unit = {
    val ds = new MemoryDataStore
    val start = new Date()
    val end = new Date(start.getTime + 1)
    val metrics = new Metrics()

    ds.persist("1", start.getTime, metrics)
    val itr = ds.find("1", start, end)

    Assert.assertTrue(itr.hasNext)
    Assert.assertEquals(metrics, itr.next())
  }

}
