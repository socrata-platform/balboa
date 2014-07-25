import sbt._
import Keys._

import sbtassembly.Plugin.AssemblyKeys._

object BalboaAdmin {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    mainClass in assembly := Some("com.socrata.balboa.jms.BalboaJms"),
    libraryDependencies ++= Seq(
      "junit" % "junit" % "4.5" % "test",
      "net.sf.opencsv" % "opencsv" % "2.0.1"
    )
  )
}
