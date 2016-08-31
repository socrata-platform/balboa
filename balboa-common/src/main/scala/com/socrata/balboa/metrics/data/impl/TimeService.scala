package com.socrata.balboa.metrics.data.impl

/**
  * Returns the current time and guarantees that successive calls
  * are well ordered. After a given time has been returned, no
  * lesser time will ever be returned.
  *
  * This is necessary to avoid backwards movement that can
  * sometimes occur in the system clock. Cassandra requires
  * timestamps to move forwards.
 */
class TimeService {
  var lastTime = 0

  def currentTimeMillis(): Long = {
    this.synchronized {
      val currentTime = System.currentTimeMillis()
      if (currentTime < lastTime) {
        lastTime
      } else {
        currentTime
      }
    }
  }
}
