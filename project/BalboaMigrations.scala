import sbt._
import Keys._

import sbtassembly.Plugin.AssemblyKeys._
import com.socrata.socratasbt.SocrataSbt._
import SocrataSbtKeys._

object BalboaMigrations {
  lazy val settings: Seq[Setting[_]] = BuildSettings.buildSettings ++ socrataProjectSettings(assembly = true) ++ Seq(
    crossScalaVersions := Seq("2.8.2", "2.10.1"),
    libraryDependencies ++= Seq(
      "net.sf.opencsv" % "opencsv" % "2.0.1",
      "commons-collections" % "commons-collections" % "3.1",
      "log4j" % "log4j" % "1.2.16"
    ),
    jarName in assembly <<= name(_ + "-jar-with-dependencies.jar")
  )
}

