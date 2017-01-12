package com.socrata.balboa.admin.tools

import java.io.IOException

import com.socrata.balboa.metrics.data.DataStoreFactory
import scala.collection.JavaConverters._

/**
  * List all the entity keys
  */
class Lister(dataStoreFactory: DataStoreFactory) {

  def listJava(filters: java.util.Iterator[String]): Unit = list(filters.asScala)

  @throws[IOException]
  def list(filters: Iterator[String], printer: (String => Unit) = println): Unit = { // scalastyle:ignore
    val ds = dataStoreFactory.get
    val entities = (if (filters.isEmpty) {
      ds.entities()
    } else {
      filters.flatMap(filter => ds.entities(filter))
    }).toList.distinct

    entities.foreach(printer)
  }
}
