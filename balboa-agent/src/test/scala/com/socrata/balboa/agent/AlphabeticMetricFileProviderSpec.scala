package com.socrata.balboa.agent

import java.nio.file.{Files, Path}
import java.util.Date
import com.socrata.balboa.util.FileUtils
import org.scalatest.{ShouldMatchers, WordSpec}

import scala.annotation.tailrec

/**
  * Unit tests for [[AlphabeticMetricFileProvider]].
  *
  * Created by michaelhotan on 2/5/16.
  */
class AlphabeticMetricFileProviderSpec extends WordSpec with ShouldMatchers {

  trait SingleRootDirectory {
    val rootDir = Files.createTempDirectory(s"${this.getClass.getSimpleName}-root")
    val provider = AlphabeticMetricFileProvider(rootDir)
  }

  trait BrokenFiles {
    val brokenFileNames = (1 to 100).map(i => i match {
      case i2 if i2 % 2 == 0 => s"$i2${FileUtils.BROKEN_FILE_EXTENSION.toLowerCase}"
      case i2 => s"$i2${FileUtils.BROKEN_FILE_EXTENSION.toUpperCase}"
    })
  }

  trait LockedFiles {
    val lockedFileNames = (1 to 100).map(i => i match {
      case i2 if i2 % 2 == 0 => s"$i2${FileUtils.LOCK_FILE_EXTENSION.toLowerCase}"
      case i2 => s"$i2${FileUtils.LOCK_FILE_EXTENSION.toUpperCase}"
    })
  }

  trait ImmutableFiles {
    val immutableFileNames = (1 to 100).map(i => i match {
      case i2 if i2 % 2 == 0 => s"$i2${FileUtils.IMMUTABLE_FILE_EXTENSION.toLowerCase}"
      case i2 => s"$i2${FileUtils.IMMUTABLE_FILE_EXTENSION.toUpperCase}"
    })
  }

  trait SiblingDirectories extends SingleRootDirectory {
    val siblings = (1 to 10).map(i => {
      Files.createDirectory(rootDir.resolve(s"$i"))
    })
  }

  trait NestedDirectories extends SingleRootDirectory {

    @tailrec final def createChildPaths(paths: Seq[Path], numChildren: Int): Seq[Path] = numChildren match {
      case i if i <= 0 => paths
      case i => createChildPaths(paths.head.resolve(s"$i") +: paths , numChildren - 1)
    }

    val nestedDirs = createChildPaths(Seq(rootDir), 10)
    Files.createDirectories(nestedDirs.head)

    assert(nestedDirs.forall(p => Files.exists(p) && p.toFile.isDirectory), "All nested directories must exist")
  }

  private def testProvider(provider: MetricFileProvider, expected: Set[Path]): Unit = {
    val actual = provider.provide
    assert(actual.forall(!_.isDirectory), "Provider should not be returning directories.")
    assert(actual.size == expected.size)
    assert(expected.forall(p => actual.contains(p.toFile)))
  }

  "A AlphabeticMetricFileProvider" when {
    "there is a single root directory" when {
      "it is empty" should {
        "not return any files" in new SingleRootDirectory {
          testProvider(provider, Set.empty)
        }
      }
      "there is one file" should {
        "not return any files" in new SingleRootDirectory {
          Files.createFile(rootDir.resolve("somefile"))
          testProvider(provider, Set.empty)
        }
      }
      "there are two files" should {
        "return the one that falls earlier in alphabetic order" in new SingleRootDirectory {
          val a = rootDir.resolve("a")
          Files.createFile(a)
          Files.createFile(rootDir.resolve("b"))
          testProvider(provider, Set(a))
        }
        "return the one at the earlier timestamp" in new SingleRootDirectory {
          val t1 = rootDir.resolve(s"${new Date().getTime}")
          Files.createFile(t1)
          Files.createFile(rootDir.resolve(s"${new Date().getTime + 1}"))
          testProvider(provider, Set(t1))
        }
      }
      "there are multiple files" should {
        "return all but the last element in alphabetic order" in new SingleRootDirectory {
          val files = (1 to 10).map(i => Files.createFile(rootDir.resolve(s"$i")))
          val expected = files.sortBy(_.toString).reverse.tail
          testProvider(provider, expected.toSet)
        }
      }
      "there are locked files" should {
        "not return them" in new SingleRootDirectory with LockedFiles {
          lockedFileNames.foreach(name => Files.createFile(rootDir.resolve(name)))
          testProvider(provider, Set.empty)
        }
      }
      "there are broken files" should {
        "not return them" in new SingleRootDirectory with BrokenFiles {
          brokenFileNames.foreach(name => Files.createFile(rootDir.resolve(name)))
          testProvider(provider, Set.empty)
        }
      }
      "there are immutable files" should {
        "always be returned" in new SiblingDirectories with ImmutableFiles {
          val immutableFiles = immutableFileNames.map(name => Files.createFile(rootDir.resolve(name)))
          testProvider(provider, immutableFiles.toSet)
        }
      }
    }
    "there are multiple sibling directories" when {
      "they all have at most no files" should {
        "return no files" in new SiblingDirectories {
          testProvider(provider, Set.empty)
        }
      }
      "they all have at most one file" should {
        "return no files" in new SiblingDirectories {
          val files = siblings.map(p => Files.createFile(p.resolve("somefile.txt")))
          testProvider(provider, Set.empty)
        }
      }
      "some have more then one file" should {
        "return files for the directories that have then one file" in new SiblingDirectories {
          val allFileNames = (1 to 100).map(i => s"$i.txt").sorted.reverse
          val notProvidedFiles = siblings.map(p => Files.createFile(p.resolve(allFileNames.head)))
          val toProcessFiles = allFileNames.tail.map(n => Files.createFile(siblings.head.resolve(n)))
          testProvider(provider, toProcessFiles.toSet)
        }
      }
      "all have more then one file" should {
        "return all but one of the files for the directories" in new SiblingDirectories {
          val allFileNames = (1 to 100).map(i => s"$i.txt").sorted.reverse
          val allFiles = siblings.flatMap(p => allFileNames.map(name => Files.createFile(p.resolve(name)))).groupBy(p => p.getParent)
          val toProcessFiles = allFiles.flatMap { case (p, files) => files.sortBy(_.toString).reverse.tail }
          testProvider(provider, toProcessFiles.toSet)
        }
      }
    }
    "there are multiple nested directories" when {
      "they all have at most no files" should {
        "return no files" in new NestedDirectories {
          testProvider(provider, Set.empty)
        }
      }
      "they all have at most one file" should {
        "return no files" in new NestedDirectories {
          val files = nestedDirs.map(p => Files.createFile(p.resolve("somefile.txt")))
          testProvider(provider, Set.empty)
        }
      }
      "some have more then one file" should {
        "return files for the directories that have then one file" in new NestedDirectories {
          val allFileNames = (1 to 100).map(i => s"$i.txt").sorted.reverse
          val notProvidedFiles = nestedDirs.map(p => Files.createFile(p.resolve(allFileNames.head)))
          val toProcessFiles = allFileNames.tail.map(n => Files.createFile(nestedDirs.head.resolve(n)))
          testProvider(provider, toProcessFiles.toSet)
        }
      }
      "all have more then one file" should {
        "return all but one of the files for the directories" in new NestedDirectories {
          val allFileNames = (1 to 100).map(i => s"$i.txt").sorted.reverse
          val allFiles = nestedDirs.flatMap(p => allFileNames.map(name => Files.createFile(p.resolve(name)))).groupBy(p => p.getParent)
          val toProcessFiles = allFiles.flatMap { case (p, files) => files.sortBy(_.toString).reverse.tail }
          testProvider(provider, toProcessFiles.toSet)
        }
      }
    }
  }
}
