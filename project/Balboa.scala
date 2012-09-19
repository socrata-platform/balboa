import sbt._

object Balboa extends Build {
  lazy val balboa = Project(
    "balboa",
    file("."),
    settings = BuildSettings.buildSettings ++ Seq(Keys.parallelExecution := false)
  ) aggregate (balboaCore, balboaHttp, balboaJms, balboaAdmin)

  lazy val balboaCore = Project(
    "balboa-core",
    file("balboa-core"),
    settings = BalboaCore.settings
  )

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
}
