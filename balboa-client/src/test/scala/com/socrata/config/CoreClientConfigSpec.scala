import java.nio.file.Files

import com.socrata.balboa.config.CoreClientConfig
import com.socrata.balboa.metrics.config.{Configuration, Keys}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, WordSpec}

class CoreClientConfigSpec extends WordSpec with BeforeAndAfterAll with BeforeAndAfter with CoreClientConfig {

  override protected def beforeAll(): Unit = {
    // Notify the runtime environment that we are currently in testing mode.
    System.getProperties.setProperty("socrata.env", "test")
  }

  override protected def before(fun: => Any): Unit = super.before(fun)

  override protected def after(fun: => Any): Unit = super.after(fun)

  "A CoreClientConfig instance" should {

    "return a directory if it is defined in the configuration" in {
      val dir = Files.createTempDirectory("test-dir")
      Configuration.get().setProperty(Keys.BACKUP_DIR, dir.toAbsolutePath.toString)
      assert(emergencyBackUpDir != null)
      assert(emergencyBackUpDir.isDirectory)
      Files.delete(dir)
    }

    "throw an IllegalStateException if a file exists and is not convertable to a directory" in {
      val file = Files.createTempFile("test-dir", null)
      Configuration.get().setProperty(Keys.BACKUP_DIR, file.toAbsolutePath.toString)
      intercept[IllegalStateException] {
        emergencyBackUpDir
      }
      Files.delete(file)
    }

    "Use an existing file for emergency writes" in {
      val dir = Files.createTempDirectory("test-dir")
      Configuration.get().setProperty(Keys.BACKUP_DIR, dir.toAbsolutePath.toString)
      assert(dir.resolve("test-file").toFile.createNewFile(), "Unable to create test file")
      val f = emergencyBackUpFile("test-file")
      assert(f.exists)
      assert(f.canWrite)
      f.delete()
      Files.delete(dir)
    }

    "Create an emergency file when it does not exists" in {
      val dir = Files.createTempDirectory("test-dir")
      Configuration.get().setProperty(Keys.BACKUP_DIR, dir.toAbsolutePath.toString)
      val f = emergencyBackUpFile("test-file")
      assert(f.exists)
      assert(f.canWrite)
      f.delete()
      Files.delete(dir)
    }
  }
}