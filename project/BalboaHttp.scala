import Dependencies._
import com.rojoma.json.util.JsonUtil._
import com.rojoma.simplearm.util._
import com.socrata.cloudbeessbt.SocrataCloudbeesSbt.SocrataSbtKeys._
import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin._

import scala.sys.process.Process

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

  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly = true) ++ net.virtualvoid.sbt.graph.Plugin.graphSettings ++ Seq(
    mainClass in assembly := Some("com.socrata.balboa.server.Main"),
    libraryDependencies <++= scalaVersion {libraries(_)},
    resourceGenerators in Compile <+= (resourceManaged in Compile, version in Compile, scalaVersion in Compile) map tagVersion,
    mergeStrategy in assembly := {
      case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
      case PathList("org","slf4j","impl", xs @ _*) => MergeStrategy.first
      case "about.html" => MergeStrategy.discard
      case x =>
        val oldStrategy = (mergeStrategy in assembly).value
        oldStrategy(x)
    },
    dependenciesSnippet :=
      <xml.group>
        <exclude org="javax.servlet" module="servlet-api" />
        <conflict org="com.socrata" manager="latest-compatible" />
        <conflict org="com.rojoma" manager="latest-compatible" />
      </xml.group>
  )

  def libraries(implicit scalaVersion: String) = Seq(
    junit,
    rojoma_json,
    simple_arm,
    socrata_http
  )
}
