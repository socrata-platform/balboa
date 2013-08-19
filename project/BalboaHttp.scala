import sbt._
import Keys._

import sbtassembly.Plugin.AssemblyKeys._
import com.socrata.socratasbt.SocrataSbt._
import SocrataSbtKeys._
import sys.process.Process
import com.rojoma.simplearm.util._
import com.rojoma.json.util.JsonUtil._

object BalboaHttp {

  def tagVersion(resourceManaged: File, version: String, scalaVersion: String): Seq[File] = {
    val file = resourceManaged / "version"
    val revision = Process(Seq("git", "describe", "--always", "--dirty")).!!.split("\n")(0)

    val result = Map(
      "version" -> version,
      "revision" -> revision,
      "scala" -> scalaVersion
    ) ++ Option(System.getenv("BUILD_TAG")).map("build" -> _)

    resourceManaged.mkdirs()
    for {
      stream <- managed(new java.io.FileOutputStream(file))
      w <- managed(new java.io.OutputStreamWriter(stream, "UTF-8"))
    } {
      writeJson(w, result, pretty = true)
      w.write("\n")
    }

    Seq(file)
  }

  lazy val settings: Seq[Setting[_]] = BuildSettings.buildSettings ++ socrataProjectSettings(assembly = true) ++ net.virtualvoid.sbt.graph.Plugin.graphSettings ++ Seq(
    libraryDependencies ++= Seq(
      "com.socrata" %% "socrata-http" % "1.3.2",
      "com.rojoma" %% "rojoma-json" % "2.1.0",
      "junit" % "junit" % "4.5" % "test"
    ),
    resourceGenerators in Compile <+= (resourceManaged in Compile, version in Compile, scalaVersion in Compile) map tagVersion,
    jarName in assembly <<= name(_ + "-jar-with-dependencies.jar"),
    dependenciesSnippet :=
      <xml.group>
        <exclude org="javax.servlet" module="servlet-api" />
      </xml.group>
  )
}
