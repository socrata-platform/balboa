import com.socrata.cloudbeessbt.SocrataCloudbeesSbt.SocrataSbtKeys._
import sbt.Keys._
import sbt._

import sbtassembly.Plugin.AssemblyKeys._

object BalboaKafka {

  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ Seq(
    mainClass in assembly := Some("com.socrata.balboa.kafka.Main"),
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka" % "0.8.2.0",
      "com.github.scopt" %% "scopt" % "3.3.0"
    ),
    dependenciesSnippet :=
      <xml.group>
        <exclude org="commons-logging" module="commons-logging"/>
      </xml.group>
  )
}