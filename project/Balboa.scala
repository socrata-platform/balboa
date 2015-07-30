import com.socrata.eu.diversit.sbt.plugin.WebDavPlugin.WebDav
import com.typesafe.sbt.packager.debian.JDebPackaging
import com.typesafe.sbt.packager.universal.UniversalPlugin
import sbt._

object Balboa extends Build {

  // WebDav.mkcol := {} Stops this library from trying to publish with WebDav.
  lazy val balboa = Project(
    projectName,
    file("."),
    settings = BuildSettings.buildSettings ++ Seq(Keys.parallelExecution := false, WebDav.mkcol := {})
  ) aggregate (balboaCommon, balboaCore, balboaHttp, balboaServiceCore, balboaJmsService,
    balboaAdmin, balboaAgent, balboaJMSClient, balboaKafkaClient,
    balboaKafkaService, balboaClientDispatcher) // Add new project definitions here.

  lazy val balboaCommon = project("balboa-common", BalboaCommon)

  lazy val balboaCore = project("balboa-core", BalboaCore, balboaCommon % "test->test;compile->compile")

  lazy val balboaHttp = project("balboa-http", BalboaHttp, balboaCore % "test->test;compile->compile")

  lazy val balboaServiceCore = project("balboa-service-core", BalboaService, balboaCore)

  // Keep the same project name for dependency reasons.
  // All following services should be name balboa-service-<type of service>
  lazy val balboaJmsService = project("balboa-service-jms", BalboaJms, balboaServiceCore)

  lazy val balboaAdmin = project("balboa-admin", BalboaAdmin, balboaCore)

  lazy val balboaAgent = project("balboa-agent", BalboaAgent, balboaJMSClient, balboaCommon)
    .enablePlugins(UniversalPlugin)
    .enablePlugins(JDebPackaging)

  lazy val balboaClientCore = project("balboa-client", BalboaClientCore,
    balboaCommon % "test->test;compile->compile")

  // Keep the same project name for dependency reasons.
  // All following clients should be name balboa-client-<type of client>
  lazy val balboaJMSClient = project("balboa-client-jms", BalboaClientJMS, balboaClientCore,
    balboaClientCore % "test->test;compile->compile")

  lazy val balboaKafkaClient = project("balboa-client-kafka", BalboaClientKafka,
    balboaClientCore, balboaClientCore % "test->test;compile->compile")

  lazy val balboaKafkaService = project("balboa-service-kafka",
    BalboaKafka, balboaServiceCore, balboaKafkaClient % "test->test;compile->compile")

  lazy val balboaClientDispatcher = project("balboa-client-dispatcher",
    BalboaClientDispatcher,
    balboaJMSClient % "test->test;compile->compile", balboaKafkaClient % "test->test;compile->compile")

  // NOTE: Add your new project or submodule here.

  // Private Helper Methods

  val projectName: String = "balboa"

  /**
   * Wrapper method that creates a project.
   *
   * @param name Name of the project
   * @param settings Object that contains settings method implementation.
   * @param dependencies List of project dependency references.
   * @return An object defining this project
   */
  private def project(name: String, settings: {
    def settings: Seq[Setting[_]] }, dependencies: ClasspathDep[ProjectReference]*) =
    Project(name, file(name), settings = settings.settings) dependsOn(dependencies: _*)
}
