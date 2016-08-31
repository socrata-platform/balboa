package com.socrata.balboa.metrics.data.impl

import com.socrata.balboa.metrics.data.DataStore
import org.slf4j.LoggerFactory

trait DataStoreImpl extends DataStore {
  private val log = LoggerFactory.getLogger(classOf[DataStoreImpl])

  def heartbeat: Unit = {}
  def ensureStarted: Unit = {}
  def onStart: Unit = log.error("Received start message from watchdog")
  def onStop: Unit = log.error("Received stop message from watchdog")
}
