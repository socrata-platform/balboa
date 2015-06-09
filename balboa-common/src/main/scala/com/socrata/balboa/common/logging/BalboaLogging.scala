package com.socrata.balboa.common.logging

import org.slf4j.{Logger, LoggerFactory}

/**
 * Consolidation of how application level logs are created.  Currently using SLF4J
 */
trait BalboaLogging {

  /*
  Developer Notes:

  Logging libraries are known for causing dependency collision hell.  In order to prevent this we centralize the way
  we provide a logger in the Balboa Common.  We then leverage the Mix In pattern that Scala natively supports.  This
  then allows us less duplication and a out of sight out of mind logging abstraction (Or atleast one step closer).

  We then provide a instance of the trait for more flexibly instantiation.
   */

  /**
   * Return the singleton Logger instance for this class.
   */
  final lazy val logger = forClass(this.getClass)

  /**
   * Returns a logger for a specific class.
   * 
   * @param clazz Class to create logger for
   * @tparam T Implicit type of the class.
   * @return The Logger instance for the argument class.
   */
  final def forClass[T](clazz: Class[T]): Logger = LoggerFactory.getLogger(clazz)
  
}

/**
 * Class used for Java based instantiation.
 */
class BalboaLoggingForJava extends BalboaLogging
