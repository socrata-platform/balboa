package com.socrata.balboa.metrics.data

import java.io.IOException

import com.socrata.balboa.metrics.config.ConfigurationException
import com.socrata.balboa.metrics.data.impl._
import com.typesafe.config.{Config, ConfigFactory}

trait DataStoreFactory {
  def get: DataStore
  def get(conf: Config): DataStore
}

object DefaultDataStoreFactory extends DataStoreFactory {

  private lazy val defaultDataStore: DataStore = try {
    get(ConfigFactory.load())
  } catch {
    case ioe: IOException =>
      throw new
          ConfigurationException(
            "Unable to determine which datastore to use because the configuration couldn't be read.", ioe)
  }

  def self: DataStoreFactory = this // For convenient Java access

  def get: DataStore = defaultDataStore

  def get(conf: Config): DataStore = {
    lazy val datastore: String = conf.getString("balboa.datastore")
    lazy val bufferGranularity: Long = conf.getLong("buffer.granularity")

    datastore match {
      case "buffered-cassandra" =>
        new BufferedDataStore(
          new BadIdeasDataStore(
            new CassandraDataStore(
              new CassandraQueryImpl(
                CassandraUtil.initializeContext(conf)))), bufferGranularity = bufferGranularity)
      case "cassandra" =>
        new BadIdeasDataStore(
          new CassandraDataStore(
            new CassandraQueryImpl(
              CassandraUtil.initializeContext(conf))))
      case "raw" =>
        new CassandraDataStore(
          new CassandraQueryImpl(
            CassandraUtil.initializeContext(conf)))
      case _ =>
        throw new ConfigurationException("Unknown datastore '" + datastore + "'.")
    }
  }
}
