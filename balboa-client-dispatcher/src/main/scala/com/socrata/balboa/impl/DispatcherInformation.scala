package com.socrata.balboa.impl

import com.socrata.metrics.components.MessageQueueComponent

/**
 * Type that defines how a dispatcher should be configured and function.
 */
trait DispatcherInformation {

  /**
   * @return Iterable collection of Message Queue Components.
   */
  def components: Iterable[MessageQueueComponent]

}
