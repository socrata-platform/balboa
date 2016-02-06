package com.blist.metrics.impl.queue

import java.nio.file.Files

import com.socrata.balboa.metrics.Metric.RecordType
import com.socrata.balboa.util.FileUtils
import org.scalatest.WordSpec

/**
  * Created by michaelhotan on 2/5/16.
  */
class MetricFileQueueSpec extends WordSpec {

  trait MetricFileQueueProvider {
    val directory = Files.createTempDirectory(this.getClass.getSimpleName)
    val metricFileQueue = new MetricFileQueue(directory.toFile, 0)
  }

  "A MetricFileQueue" when {
    "closing a file" should {
      "rename the file with correct suffix" in new MetricFileQueueProvider {
        metricFileQueue.create("entitiy-id", "metric-name-1", 1, 1, RecordType.AGGREGATE)
        metricFileQueue.create("entitiy-id", "metric-name-2", 1, 1, RecordType.AGGREGATE)
        val completedFiles = directory.toFile.listFiles().filter(f => f.getAbsolutePath.endsWith(FileUtils.IMMUTABLE_FILE_EXTENSION))
        assert(completedFiles.length == 1)
      }
    }
  }
}
