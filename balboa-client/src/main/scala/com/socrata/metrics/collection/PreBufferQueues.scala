package com.socrata.metrics.collection

import java.util.concurrent.LinkedBlockingQueue

import com.socrata.metrics.components.MetricEntry

trait PreBufferQueue {
  def queue:SocrataAbstractQueue[MetricEntry]
}

trait LinkedBlockingPreBufferQueue extends PreBufferQueue {
  lazy val queue = new SocrataLinkedBlockingQueue[MetricEntry](new LinkedBlockingQueue[MetricEntry]())
}