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
    dependenciesSnippet :=
      <xml.group>
        <exclude org="org.codehaus.jackson" module="jackson-mapper-asl" />
        <exclude org="org.codehaus.jackson" module="jackson-core-asl" />
      </xml.group>,
    jarName in assembly <<= name(_ + "-jar-with-dependencies.jar")
  )
}
