package com.socrata.balboa.config

import java.io.File
import java.nio.file.{Files, Paths}

import com.socrata.balboa.metrics.config.{Configuration, Keys}

/**
 * Core Client Configuration.
 */
trait CoreClientConfig {

  /**
   * @return The emergency file directory
   */
  def emergencyBackUpDir: File = {
    val p = Paths.get(Configuration.get().getString(Keys.BACKUP_DIR))
    if (!Files.exists(p)) {
      Files.createDirectory(p)
    } else if (!Files.isDirectory(p)) {
      if (!p.toFile.mkdir())
        throw new IllegalStateException(s"$p exists and is not a directory.")
    }
    p.toFile
  }

  /**
   * Creates or returns existing emergency file from a given name.
   *
   * @param name Emergency File short name.
   */
  def emergencyBackUpFile(name: String) = {
    val p = Paths.get(emergencyBackUpDir.getAbsolutePath).resolve(name)
    if (!Files.exists(p))
      Files.createFile(p)
    p.toFile
  }

}
