import sbt._
import Keys._

import sbtassembly.Plugin.AssemblyKeys._
import com.socrata.socratasbt.SocrataSbt._

object BalboaHttp {
  lazy val settings: Seq[Setting[_]] = BuildSettings.buildSettings ++ socrataProjectSettings(assembly = true) ++ Seq(
    libraryDependencies ++= Seq(
      "com.socrata" %% "socrata-http" % "1.0.0",
      "com.rojoma" %% "rojoma-json" % "2.0.0",
      "junit" % "junit" % "4.5" % "test"
    ),
    jarName in assembly <<= name(_ + "-jar-with-dependencies.jar")
  )
}
