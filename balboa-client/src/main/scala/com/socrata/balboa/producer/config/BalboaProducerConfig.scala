package com.socrata.balboa.producer.config

import java.io.File
import java.nio.file.attribute.{PosixFilePermission, PosixFilePermissions}
import java.nio.file.{Files, Path, Paths}

import com.socrata.balboa.common.config.{Configuration, Keys}

import scala.collection.JavaConversions._

/**
 * Parent Configuration class for all Client Configurations.  This class encapsulates core functionality that can be
 * shared between all clients.
 */
trait BalboaProducerConfig {

  /**
   * All emergency messages will be placed under a common parent directory.  This function returns that directory.
   *  This function will attempt to use the directory referenced [[Keys.BACKUP_DIR]].  If the directory does not
   *  exists then it will be created.
   *
   * @return The emergency file directory.
   * @throws IllegalStateException If the file exists but is not a directory.
   */
  def emergencyBackUpDir: File = Paths.get(Configuration.get().getString(Keys.BACKUP_DIR)) match {
    case p: Path if !Files.exists(p) => Files.createDirectory(p).toFile
    case p: Path if Files.exists(p) && Files.isDirectory(p) => p.toFile
    case p: Path if Files.exists(p) && !Files.isDirectory(p) && !p.toFile.mkdir() =>
      throw new IllegalStateException(s"$p exists and cannot be converted to a directory.")
    // Should not reach this point if it does then there is some error in the logic
  }

  /**
   * Creates or returns existing emergency file from a given name.
   *
   * @param name Emergency File short name.
   */
  def emergencyBackUpFile(name: String): File = emergencyBackUpDir.toPath.resolve(name) match {
    case p: Path if Files.exists(p) && Files.isWritable(p) => p.toFile
    case p: Path if Files.exists(p) && !Files.isWritable(p) && !p.toFile.setWritable(true) => throw new
        IllegalStateException(s"$p exists but is not and could not make that file writable")
    case p: Path if !Files.exists(p) =>
      Files.createFile(p, PosixFilePermissions.asFileAttribute(Set(
        PosixFilePermission.GROUP_READ,
        PosixFilePermission.OTHERS_READ,
        PosixFilePermission.OWNER_READ,
        PosixFilePermission.OWNER_WRITE))).toFile
    // Again should never reach this point.  If it does, obvious logic error.
  }

}
