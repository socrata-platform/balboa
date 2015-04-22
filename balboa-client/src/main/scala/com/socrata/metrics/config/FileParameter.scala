package com.socrata.metrics.config

import java.io.File

/**
 * Mixinable trait that can be utilized to provide a general interface
 * for having a file requirement as parameter to a component.
 */
trait FileParameter {

  /**
   * @return The File Parameter.
   */
  def file: File

}

/**
 * Mixinable Emergency file parameter meant to be used with component
 * pattern.  This is a convenience extension of [[FileParameter]] and
 * was created for a more definitive abstraction.
 */
trait EmergencyFileParameter extends FileParameter {

  /**
   * Create a naming convention that easily identifies this as a parameter.
   *
   * @return The emergency file.
   */
  def emergencyFile: File = file

}