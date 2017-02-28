package com.socrata.balboa.util

import java.io.{File, FileFilter}

/**
  * Created by michaelhotan on 2/5/16.
  */
object FileUtils {

  // scalastyle:off field.name

  val BROKEN_FILE_EXTENSION = ".broken"

  val LOCK_FILE_EXTENSION = ".lock"

  val IMMUTABLE_FILE_EXTENSION = ".completed"

  // scalastyle:on field.name

  val isBalboaDataFile = new FileFilter() {
    override def accept(file: File): Boolean = file match {
      case _ if file.isDirectory => false
      case _ if isBalboaImmutableFile.accept(file) => true
      case _ if !(isBalboaBrokenFile.accept(file) || isBalboaLockFile.accept(file)) => true
      case _ => false
    }
  }

  val isBalboaBrokenFile: FileFilter = isFileEndingWith(BROKEN_FILE_EXTENSION)

  val isBalboaLockFile: FileFilter = isFileEndingWith(LOCK_FILE_EXTENSION)

  val isBalboaImmutableFile: FileFilter = isFileEndingWith(IMMUTABLE_FILE_EXTENSION)

  val isDirectory = new FileFilter() {
    override def accept(file: File): Boolean = file.isDirectory
  }

  def isFileEndingWith(suffix: String): FileFilter = {
    new FileFilter {
      override def accept(file: File): Boolean = file.isFile && file.getName.toLowerCase.endsWith(suffix)
    }
  }

  def getBalboaDataFilesAtRoot(dir: File): Set[File] = Set(dir.listFiles(isBalboaDataFile):_*)

  /**
    * Recursively retrieves all the child directories within this one.
    *
    * @param rootDir The root directory search
    * @return The set of all directories including the root.  Empty if `rootDir` is not a directory.
    */
  def getDirectories(rootDir: File): Set[File] = rootDir match {
    case dir: File if dir.isDirectory => dir.listFiles().flatMap(f => getDirectories(f)).toSet + dir
    case _ => Set.empty
  }

  def getFiles(rootDir: File, fileFilter: Option[FileFilter]): Set[File] = getDirectories(rootDir).flatMap(dir =>
    fileFilter match {
      case Some(f) => dir.listFiles(f)
      case _ => dir.listFiles()
    })

}
