package com.socrata.balboa.util

import java.io.{File, FileFilter}

/**
  * Created by michaelhotan on 2/5/16.
  */
object FileUtils {

  val BROKEN_FILE_EXTENSION = ".broken"

  val LOCK_FILE_EXTENSION = ".lock"

  val IMMUTABLE_FILE_EXTENSION = ".completed"

  val isBalboaDataFile = new FileFilter() {
    override def accept(file: File): Boolean = file match {
      case f if f.isDirectory => false
      case f if isBalboaImmutableFile.accept(f) => true
      case f if !(isBalboaBrokenFile.accept(file) || isBalboaLockFile.accept(file)) => true
      case _ => false
    }
  }

  val isBalboaBrokenFile = new FileFilter {
    override def accept(file: File): Boolean = file.isFile &&
      (file.getName.endsWith(BROKEN_FILE_EXTENSION) || file.getName.endsWith(BROKEN_FILE_EXTENSION.toUpperCase))
  }

  val isBalboaLockFile = new FileFilter {
    override def accept(file: File): Boolean = file.isFile &&
      (file.getName.endsWith(LOCK_FILE_EXTENSION) || file.getName.endsWith(LOCK_FILE_EXTENSION.toUpperCase))
  }

  val isBalboaImmutableFile = new FileFilter {
    override def accept(file: File): Boolean = file.isFile &&
      (file.getName.endsWith(IMMUTABLE_FILE_EXTENSION) || file.getName.endsWith(IMMUTABLE_FILE_EXTENSION.toUpperCase))
  }

  val isDirectory = new FileFilter() {
    override def accept(file: File): Boolean = file.isDirectory
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
