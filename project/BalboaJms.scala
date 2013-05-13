import sbt._
import Keys._

import sbtassembly.Plugin.AssemblyKeys._
import com.socrata.socratasbt.SocrataSbt._
import SocrataSbtKeys._

object BalboaJms {
  lazy val settings: Seq[Setting[_]] = BuildSettings.buildSettings ++ socrataProjectSettings(assembly = true) ++ Seq(
    libraryDependencies ++= Seq(
      "org.apache.activemq" % "activemq-core" % "5.2.0"
    ),
    jarName in assembly <<= name(_ + "-jar-with-dependencies.jar")
  )
}
