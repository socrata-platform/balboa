import sbt._

object Balboa extends Build {
  lazy val balboa = Project(
    projectName,
    file("."),
    settings = BuildSettings.buildSettings ++ Seq(Keys.parallelExecution := false)
  ) aggregate (allOtherProjects: _*)

  lazy val balboaCommon = project("balboa-common", None, BalboaCommon)

  lazy val balboaCore = project("balboa-core", None, BalboaCore, balboaCommon)

  lazy val balboaHttp = project("balboa-http", None, BalboaHttp, balboaCore)

  lazy val balboaJms = project("balboa-jms", None, BalboaJms, balboaCore)

  lazy val balboaAdmin = project("balboa-admin", None, BalboaAdmin, balboaCore)

  lazy val balboaClientCore = project("balboa-client-core", Some("balboa-client"), BalboaClient, balboaCommon)

  lazy val balboaClientJMS = project("balboa-client", Some("balboa-client-jms"), BalboaClientJMS, balboaClientCore)

  lazy val balboaClientKafka = project("balboa-kafka-client", Some("balboa-client-kafka"), BalboaClientKafka, balboaClientCore)

  lazy val balboaKafka = project("balboa-kafka", None, BalboaKafka, balboaCore)

  // Private Helper Methods

  val projectName: String = "balboa"

  /**
   * @return A reference to all the project instances for this build
   */
  private def allOtherProjects = for {
    method <- getClass.getDeclaredMethods.toSeq
    if method.getParameterTypes.isEmpty && classOf[Project].isAssignableFrom(method.getReturnType) && method.getName != projectName
  } yield method.invoke(this).asInstanceOf[Project] : ProjectReference

  /**
   * Wrapper method that creates a project.  If the directory name of the project.
   *
   * @param name Name of the project
   * @param directoryName If the directory name is different the project name
   * @param settings Object that contains settings variable
   * @param dependencies List of project dependency references.
   * @return An object defining this project
   */
  private def project(name: String, directoryName: Option[String], settings: { def settings: Seq[Setting[_]] }, dependencies: ClasspathDep[ProjectReference]*) =
    Project(name, directoryName match {
      case Some(dir) => file(dir)
      case None => file(name)
    }, settings = settings.settings) dependsOn(dependencies: _*)
}
