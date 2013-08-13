package com.socrata.metrics.collection

import com.socrata.metrics.components.MetricEntry
import java.util.concurrent.LinkedBlockingQueue

trait PreBufferQueue {
  def queue:SocrataAbstractQueue[MetricEntry]
}

trait LinkedBlockingPreBufferQueue extends PreBufferQueue {
  lazy val queue = new SocrataLinkedBlockingQueue[MetricEntry](new LinkedBlockingQueue[MetricEntry]())
}