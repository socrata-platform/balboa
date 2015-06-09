import com.socrata.eu.diversit.sbt.plugin.WebDavPlugin.WebDav
import sbt._
import sbtdocker.DockerPlugin

object Balboa extends Build {

  // WebDav.mkcol := {} Stops this library from trying to publish with WebDav.
  lazy val balboa = Project(
    projectName,
    file("."),
    settings = BuildSettings.buildSettings ++ Seq(Keys.parallelExecution := false, WebDav.mkcol := {})
  ) aggregate (balboaCommon, balboaCore, balboaHttp, balboaServiceCore, balboaJms,
    balboaAdmin, balboaClientCore, balboaClientJMS, balboaKafkaCommon, balboaKafkaClient,
    balboaKafkaService, balboaClientDispatcher) // Add new project definitions here.

  lazy val balboaCommon = project("balboa-common", None, BalboaCommon)

  lazy val balboaCore = project("balboa-core", None, BalboaCore, balboaCommon % "test->test;compile->compile")

  lazy val balboaHttp = project("balboa-http", None, BalboaHttp, balboaCore % "test->test;compile->compile")

  lazy val balboaServiceCore = project("balboa-service-core", None, BalboaService, balboaCore)

  // Keep the same project name for dependency reasons.
  // All following services should be name balboa-service-<type of service>
  lazy val balboaJms = project("balboa-jms", Some("balboa-service-jms"), BalboaJms, balboaServiceCore)

  lazy val balboaAdmin = project("balboa-admin", None, BalboaAdmin, balboaCore)

  lazy val balboaAgent = project("balboa-agent", None, BalboaAgent, balboaClientJMS, balboaCommon)

  lazy val balboaClientCore = project("balboa-client-core", Some("balboa-client"), BalboaClient,
    balboaCommon % "test->test;compile->compile")

  // Keep the same project name for dependency reasons.
  // All following clients should be name balboa-client-<type of client>
  lazy val balboaClientJMS = project("balboa-client", Some("balboa-client-jms"), BalboaClientJMS, balboaClientCore,
    balboaClientCore % "test->test;compile->compile")

  lazy val balboaKafkaCommon = project("balboa-kafka-common", Some("balboa-common-kafka"), BalboaKafkaCommon,
    balboaCommon % "test->test;compile->compile")

  lazy val balboaKafkaClient = project("balboa-kafka-client", Some("balboa-client-kafka"), BalboaClientKafka,
    balboaClientCore, balboaKafkaCommon % "test->test;compile->compile", balboaClientCore % "test->test;compile->compile")

  lazy val balboaKafkaService = project("balboa-kafka-service", Some("balboa-service-kafka"),
    BalboaKafka, balboaServiceCore, balboaKafkaClient % "test->test;compile->compile")
    .enablePlugins(DockerPlugin)

  lazy val balboaClientDispatcher = project("balboa-dispatcher-client", Some("balboa-client-dispatcher"),
    BalboaClientDispatcher,
    balboaClientJMS % "test->test;compile->compile", balboaKafkaClient % "test->test;compile->compile")

  // NOTE: Add your new project or submodule here.

  // Private Helper Methods

  val projectName: String = "balboa"

  /**
   * Wrapper method that creates a project.  If the directory name of the project.
   *
   * @param name Name of the project
   * @param directoryName The name of the directory housing the project, If it is different from the project name.
   * @param settings Object that contains settings method implementation.
   * @param dependencies List of project dependency references.
   * @return An object defining this project
   */
  private def project(name: String, directoryName: Option[String], settings: {
    def settings: Seq[Setting[_]] }, dependencies: ClasspathDep[ProjectReference]*) =
    Project(name, directoryName match {
      case Some(dir) => file(dir)
      case None => file(name)
    }, settings = settings.settings) dependsOn(dependencies: _*)
}
