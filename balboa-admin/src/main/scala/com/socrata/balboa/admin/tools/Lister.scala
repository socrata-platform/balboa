package com.socrata.balboa.admin.tools

import java.io.IOException

import com.socrata.balboa.metrics.data.DataStoreFactory

/**
  * List all the entity keys
  */
class Lister(dataStoreFactory: DataStoreFactory) {
  @throws[IOException]
  def list(filters: Iterator[String]): Unit = {
    val ds = dataStoreFactory.get
    val entities = if (filters.isEmpty) {
      ds.entities()
    } else {
      filters.flatMap(filter => ds.entities(filter))
    }
    entities.foreach(println)
  }
}
