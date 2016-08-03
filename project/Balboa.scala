import com.earldouglas.xwp.JettyPlugin
import com.typesafe.sbt.packager.archetypes.JavaAppPackaging
import com.typesafe.sbt.packager.debian.DebianPlugin
import com.typesafe.sbt.packager.docker.DockerPlugin
import com.typesafe.sbt.packager.universal.UniversalPlugin
import io.gatling.sbt.GatlingPlugin
import io.gatling.sbt.GatlingKeys.GatlingIt
import sbt._

object Balboa extends Build {

  lazy val balboa = Project(
    projectName,
    file("."),
    settings = BuildSettings.buildSettings ++ Seq(Keys.parallelExecution := false)
  ) aggregate (balboaAgent, balboaAdmin, balboaCommon, balboaCore, balboaHttp, balboaServiceCore, balboaJms,
     balboaClientCore, balboaClientJMS, balboaClientDispatcher) // Add new project definitions here.

  lazy val balboaCommon = project("balboa-common", BalboaCommon)
    .enablePlugins(com.socrata.sbtplugins.BuildInfoPlugin)

  lazy val balboaCore = project("balboa-core", BalboaCore, balboaCommon)

  lazy val balboaHttp = project("balboa-http", BalboaHttp, balboaCore)
    .enablePlugins(JettyPlugin)
    .enablePlugins(GatlingPlugin)
    .configs(IntegrationTest, GatlingIt)
    .settings(Defaults.itSettings: _*)

  lazy val balboaServiceCore = project("balboa-service-core", BalboaService, balboaCore)

  // Keep the same project name for dependency reasons.
  // All following services should be name balboa-service-<type of service>
  lazy val balboaJms = project("balboa-service-jms", BalboaJms, balboaServiceCore)

  lazy val balboaAdmin = project("balboa-admin", BalboaAdmin, balboaCore)

  lazy val balboaAgent = project("balboa-agent", BalboaAgent, balboaClientJMS, balboaCommon)
    .enablePlugins(JavaAppPackaging)
    .enablePlugins(UniversalPlugin)
    .enablePlugins(DebianPlugin)
    .enablePlugins(DockerPlugin)
    .configs(IntegrationTest)
    .settings(Defaults.itSettings: _*)

  lazy val balboaClientCore = project("balboa-client", BalboaClient,
    balboaCommon % "test->test;compile->compile")

  // Keep the same project name for dependency reasons.
  // All following clients should be name balboa-client-<type of client>
  lazy val balboaClientJMS = project("balboa-client-jms", BalboaClientJMS, balboaClientCore,
    balboaClientCore % "test->test;compile->compile")

  lazy val balboaClientDispatcher = project("balboa-client-dispatcher",
    BalboaClientDispatcher,
    balboaClientJMS % "test->test;compile->compile")

  // NOTE: Add your new project or submodule here.

  // Private Helper Methods

  val projectName: String = "balboa"

  /**
   * TODO Doesn't work with SBT 13.* Currently not used.
   *
   * @return A reference to all the project instances for this build
   */
  private def allOtherProjects = for {
    method <- getClass.getDeclaredMethods.map(m => {
      //      m.setAccessible(true)
      m
    }).toSeq
    if method.getParameterTypes.isEmpty && classOf[Project].isAssignableFrom(method.getReturnType) && method.getName != projectName
  } yield {
      method.invoke(this).asInstanceOf[Project] : ProjectReference
    }

  /**
   * Wrapper method that creates a project.  If the directory name of the project.
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
