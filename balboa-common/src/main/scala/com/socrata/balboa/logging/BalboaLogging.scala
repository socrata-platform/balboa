package com.socrata.balboa.logging

import org.slf4j.LoggerFactory

/**
 * Consolidation of how application level logs are created.  Currently using SLF4J
 */
trait BalboaLogging {

  /**
   * Return the single Logger instance for this class.
   */
  lazy val logger = LoggerFactory.getLogger(this.getClass)

}

/**
 * Class used for Java based instantiation.
 */
class BalboaLoggingForJava extends BalboaLogging
