package com.socrata.balboa.agent

import java.io.File
import java.nio.file.Path
import com.socrata.balboa.util.FileUtils
import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.collection.JavaConverters._

/**
  * A Metric File Provider provides a set of Metric Data Files.
  */
trait MetricFileProvider {

  // The intent of this class was to provide a flexible mechanism to allow individual implementations to decide which
  // files to return.

  // Having this functionality encapsulated in a trait allows for easy external polymorphism.

  /**
    * @return A set of Files.
    */
  def provide: Set[File]

  /**
    * Java version of [[provide]]
 *
    * @return A [[java.util.Set]] of [[provide]]
    */
  def provideForJava: java.util.Set[File] = provide.asJava

}

/**
  * A [[MetricFileProvider]] that is sourced at a root directory.
  */
trait DirectoryBasedMetricFileProvider extends MetricFileProvider {

  /**
    * @return The path to the base root directory.
    */
  def path: Path

}

/**
  * Recursively finds, filters, and returns a set of files with a root directory based off of the alphabetic ordinality.
  *
  * See [[AlphabeticMetricFileProvider.path]].
  *
  * @param path See [[DirectoryBasedMetricFileProvider.path]]
  */
case class AlphabeticMetricFileProvider(override val path: Path) extends DirectoryBasedMetricFileProvider with LazyLogging {

  /**
    * <br>
    * Given a a file path to a root directory, this class...
    *   * Recursively searches for all internal sub directories.
    *   * For each directory, including the root, it sorts the files by name in descending order
    *   * For each sorted list, it returns the last n-1 elements.
    *
    * <br>
    * If the metric data file producer wrote data files in with any kind of alphabetic ordinality, this provider will
    * attempt to avoid the last file written.
    *
    * Reference [[MetricFileProvider.provide]]
    *
    * @return A set of Files.
    */
  override def provide: Set[File] = {
    val allFiles = FileUtils.getDirectories(path.toFile).flatMap(dir => dir.listFiles(FileUtils.isBalboaDataFile).toSet)
    // Immutable files are always as ready to process.
    val immutableFiles = allFiles.filter(FileUtils.isBalboaImmutableFile.accept(_))
    // Group all the files by its parent directory
    // Sort in descending order and remove the first element
    // Join the result of all the directories in a final set of files.
    allFiles.groupBy(_.getParent).map {
      case (parentPath, files) => (parentPath, files.toSeq.sortBy(_.getAbsolutePath).reverse.tail)
    }.flatMap(_._2).toSet ++ immutableFiles
  }
}



