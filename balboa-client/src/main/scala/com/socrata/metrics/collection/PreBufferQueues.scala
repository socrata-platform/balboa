package com.socrata.metrics.collection

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import com.socrata.metrics.components.MetricEntry

trait PreBufferQueue {
  def queue: BlockingQueue[MetricEntry]
}

trait LinkedBlockingPreBufferQueue extends PreBufferQueue {
  lazy val queue = new LinkedBlockingQueue[MetricEntry]()
}