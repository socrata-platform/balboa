package com.socrata.balboa.config

import java.io.File
import java.nio.file.attribute.{PosixFilePermission, PosixFilePermissions}
import java.nio.file.{Files, Path, Paths}

import com.socrata.balboa.metrics.config.Keys
import com.typesafe.config.Config

import scala.collection.JavaConverters._

/**
 * Parent Configuration class for all Client Configurations.  This class encapsulates core functionality that can be
 * shared between all clients.
 */
case class CoreClientConfig(conf: Config) {

  /**
   * All emergency messages will be placed under a common parent directory.  This function returns that directory.
   *  This function will attempt to use the directory referenced [[Keys.BackupDir]].  If the directory does not
   *  exists then it will be created.
   *
   * @return The emergency file directory.
   * @throws IllegalStateException If the file exists but is not a directory.
   */
  def emergencyBackUpDir: File = Paths.get(conf.getString(Keys.BackupDir)) match {
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
  def emergencyBackUpFile(name: String): File = {
    val path = emergencyBackUpDir.toPath.resolve(name)
    if (Files.exists(path)) {
      if (Files.isWritable(path).||(path.toFile.setWritable(true))) {
        path.toFile
      } else {
        throw new IllegalStateException(s"$path exists but is not and could not make that file writable")
      }
    } else {
      Files.createFile(path, PosixFilePermissions.asFileAttribute(Set(
        PosixFilePermission.GROUP_READ,
        PosixFilePermission.OTHERS_READ,
        PosixFilePermission.OWNER_READ,
        PosixFilePermission.OWNER_WRITE).asJava)).toFile
    }
  }
}
