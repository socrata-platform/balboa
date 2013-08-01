import sbt._

object Balboa extends Build {
  lazy val balboa = Project(
    "balboa",
    file("."),
    settings = BuildSettings.buildSettings ++ Seq(Keys.parallelExecution := false)
  ) aggregate (balboaCommon, balboaCore, balboaHttp, balboaJms, balboaAdmin, balboaClient, balboaMigrations)

  lazy val balboaCommon = Project(
    "balboa-common",
    file("balboa-common"),
    settings = BalboaCommon.settings
  )

  lazy val balboaCore = Project(
    "balboa-core",
    file("balboa-core"),
    settings = BalboaCore.settings
  ) dependsOn(balboaCommon)

  lazy val balboaHttp = Project(
    "balboa-http",
    file("balboa-http"),
    settings = BalboaHttp.settings
  ) dependsOn(balboaCore)

  lazy val balboaJms = Project(
    "balboa-jms",
    file("balboa-jms"),
    settings = BalboaJms.settings
  ) dependsOn(balboaCore)

  lazy val balboaAdmin = Project(
    "balboa-admin",
    file("balboa-admin"),
    settings = BalboaAdmin.settings
  ) dependsOn(balboaCore)

  lazy val balboaClient = Project(
    "balboa-client",
    file("balboa-client"),
    settings = BalboaClient.settings
  ) dependsOn(balboaCommon)
  
  lazy val balboaMigrations = Project(
    "balboa-migrations",
    file("balboa-migrations"),
    settings = BalboaMigrations.settings
  ) dependsOn(balboaCore, balboaCommon, balboaClient)
}
