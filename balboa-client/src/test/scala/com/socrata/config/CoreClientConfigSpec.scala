import java.nio.file.Files

import com.socrata.balboa.config.CoreClientConfig
import com.socrata.balboa.metrics.config.Keys
import com.typesafe.config.Config
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, WordSpec}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

class CoreClientConfigSpec extends WordSpec with BeforeAndAfterAll with BeforeAndAfter with MockitoSugar {

  override protected def beforeAll(): Unit = {
    // Notify the runtime environment that we are currently in testing mode.
    System.getProperties.setProperty("socrata.env", "test")
  }

  override protected def before(fun: => Any): Unit = super.before(fun)

  override protected def after(fun: => Any): Unit = super.after(fun)

  "A CoreClientConfig instance" should {

    "return a directory if it is defined in the configuration" in {
      val dir = Files.createTempDirectory("test-dir")
      val configMock = mock[Config]
      when(configMock.getString(Keys.BackupDir)).thenReturn(dir.toAbsolutePath.toString)
      val coreCliConf = new CoreClientConfig(configMock)
      assert(coreCliConf.emergencyBackUpDir != null)
      assert(coreCliConf.emergencyBackUpDir.isDirectory)
      Files.delete(dir)
    }

    "throw an IllegalStateException if a file exists and is not convertable to a directory" in {
      val file = Files.createTempFile("test-dir", null)
      val configMock = mock[Config]
      when(configMock.getString(Keys.BackupDir)).thenReturn(file.toAbsolutePath.toString)
      val coreCliConf = new CoreClientConfig(configMock)
      intercept[IllegalStateException] {
        coreCliConf.emergencyBackUpDir
      }
      Files.delete(file)
    }

    "Use an existing file for emergency writes" in {
      val dir = Files.createTempDirectory("test-dir")
      assert(dir.resolve("test-file").toFile.createNewFile(), "Unable to create test file")

      val configMock = mock[Config]
      when(configMock.getString(Keys.BackupDir)).thenReturn(dir.toAbsolutePath.toString)
      val coreCliConf = new CoreClientConfig(configMock)

      val f = coreCliConf.emergencyBackUpFile("test-file")
      assert(f.exists)
      assert(f.canWrite)
      f.delete()
      Files.delete(dir)
    }

    "Create an emergency file when it does not exists" in {
      val dir = Files.createTempDirectory("test-dir")

      val configMock = mock[Config]
      when(configMock.getString(Keys.BackupDir)).thenReturn(dir.toAbsolutePath.toString)
      val coreCliConf = new CoreClientConfig(configMock)

      val f = coreCliConf.emergencyBackUpFile("test-file")
      assert(f.exists)
      assert(f.canWrite)
      f.delete()
      Files.delete(dir)
    }
  }
}