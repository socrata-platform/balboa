package com.socrata.balboa.server

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.StrictLogging
import org.scalatra.ScalatraServlet

trait ClientCounter extends ScalatraServlet with StrictLogging {
  before() {
    ClientCounter.incr()
  }

  after() {
    ClientCounter.decr()
  }
}

object ClientCounter {
  var counter = new AtomicInteger(0)

  def incr(): Unit = {
    counter.addAndGet(1)
  }

  def decr(): Unit = {
    counter.addAndGet(-1)
  }

  def get(): Int = {
    counter.get()
  }
}
