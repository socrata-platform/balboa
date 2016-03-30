import Dependencies._
import sbt.Keys._
import sbt.project
import GitRevision.gitCommit
import com.typesafe.sbt.packager.docker.{Cmd, ExecCmd}

val JavaVersion = "1.8"
val MaintainerEmail = "mission-control-l@socrata.com"

/**
  * Settings that are common to all Balboa projects.
  */
lazy val commonSettings = Seq(
  organization := "com.socrata",
  organizationName := "Socrata",
  organizationHomepage := Some(new URL("https://www.socrata.com")),
  scalaVersion := "2.11.7",
  javacOptions ++= Seq("-source", JavaVersion, "-target", JavaVersion, "-Xlint"),
  initialize := {
    val _ = initialize.value
    if (sys.props("java.specification.version") != JavaVersion)
      sys.error(s"Java $JavaVersion is required for this project.")
  },
  // Minimum required test coverage or the 'clean coverage test' task will fail.
  // Feel free to increase this whenever you notice the coverage number has
  // gone up (visible in the output of the 'sbt clean coverage build' task).
  // However, this number should never go down. If it appears that new code is
  // added which does not need to be tested (boilerplate type code) find a way
  // to exclude it from measured coverage rather than decrease coverage.
  // NOTE: Reviewers should challenge any decrease in this number.
  // NOTE: This required test coverage effects all sub projects
  scoverage.ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 70,
  // TODO: Uncomment when Balboa is in a state where it can meet the above test coverage(mentioned above)
  scoverage.ScoverageSbtPlugin.ScoverageKeys.coverageFailOnMinimum := false,

  // These are currently flags that prevent Findbug errors from failing the build.
  // We will most likely remove the below lines to allow us to build these types of checks
  // into our continuous deploy process.
  com.socrata.sbtplugins.findbugs.JavaFindBugsPlugin.JavaFindBugsKeys.findbugsFailOnError in Compile := false,
  com.socrata.sbtplugins.findbugs.JavaFindBugsPlugin.JavaFindBugsKeys.findbugsFailOnError in Test := false,

  // Do not fail the build on style errors.
  com.socrata.sbtplugins.StylePlugin.StyleKeys.styleFailOnError in Compile := false,

  fork in test := true,
  // Some Balboa tests rely on the below runtime environment variable.
  javaOptions in test += "-Dsocrata.env=test",

  // TODO Remove if not needed
  // Related Issue: http://scala-language.1934581.n4.nabble.com/Scaladoc-2-11-quot-throws-tag-quot-cannot-find-any-member-to-link-td4641850.html
  scalacOptions in (Compile, doc) ++= Seq(
    "-no-link-warnings" // Suppresses problems with Scaladoc @throws links
  ),

  ////////////////////////////////////////////////////////////////////////////////////
  //// Build Info plugin settings

  buildInfoPackage := s"com.socrata.${name.value.replaceAllLiterally("-",".")}",
  buildInfoKeys ++= Seq(
    BuildInfoKey.action("gitCommit") { gitCommit }
  ),

  ////////////////////////////////////////////////////////////////////////////////////
  //// Required for Scala Doc.

  autoAPIMappings := true,
  apiMappings ++= {
    // Lookup the path to jar (it's probably somewhere under ~/.ivy/cache) from computed classpath
    val classpath = (fullClasspath in Compile).value
    def findJar(name: String): File = {
      val regex = ("/" + name + "[^/]*.jar$").r
      classpath.find { jar => regex.findFirstIn(jar.data.toString).nonEmpty }.get.data // fail hard if not found
    }

    // Define external documentation paths
    Map(
      findJar("scala-library") -> url("http://scala-lang.org/api/" + scalaVersion + "/")
    )
  }

  ////////////////////////////////////////////////////////////////////////////////////
)

// Default settings for Ivy artifacts
lazy val ivySettings = Seq(
  crossScalaVersions := Seq("2.10.6", "2.11.8"),
  publishTo := {
    val nexus = "https://repo.socrata.com/artifactory/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "libs-snapshot-local")
    else
      Some("releases"  at nexus + "libs-release-local")
  }
)

// Socrata Docker support
// The following configuration allows this project to utilize the Socrata base image
// to build a docker image for running th respective project.
lazy val dockerSettings = Seq(
  packageName in Docker := s"socrata/${name.value}",
  maintainer in Docker := MaintainerEmail,
  version in Docker := gitCommit,
  dockerBaseImage := "socrata/runit-java8",
  daemonUser in Docker := "root", // Required to change permissions of executable files.
  dockerUpdateLatest in Docker := true,
  // In order to support the Socrata execution model required in the runit base
  // images, we must create run and log executable file.  These files will be used
  // by runit in the base image to manage the process lifecycle.
  resourceGenerators in Compile ++= Seq(
    // In order
    Def.task {
      val file = (resourceManaged in Compile).value / "scripts" / "run"
      val scriptLocation: String = s"${(defaultLinuxInstallLocation in Docker).value}/bin/${(executableScriptName in Universal).value}"
      val contents =
        s"""#!/bin/sh
            |# NOTE: Generated Script.  Please do NOT manually augment.
            |set -e
            |[ -n "$${LOG4J_LEVEL}" ] && JAVA_OPTS="$${JAVA_OPTS} -Dlog4j.logLevel=$${LOG4J_LEVEL}"
            |exec /sbin/setuser socrata $scriptLocation
        """.stripMargin
      IO.write(file, contents)
      file.setExecutable(true)
      Seq(file)
    }.taskValue,
    Def.task {
      val file = (resourceManaged in Compile).value / "scripts" / "log"
      val contents =
        s"""#!/bin/sh
            |# NOTE: Generated Script.  Please do NOT manually augment.
            |exec svlogd -tt /var/log/${name.value}
        """.stripMargin
      IO.write(file, contents)
      file.setExecutable(true)
      Seq(file)
    }.taskValue
  ),
  mappings in Docker ++= Seq(
    (resourceManaged in Compile).value / "scripts" / "run" -> s"/etc/service/${name.value}/run",
    (resourceManaged in Compile).value / "scripts" / "log" -> s"/etc/service/${name.value}/log"
  ),
  dockerCommands := dockerCommands.value.filterNot {
    case ExecCmd("ENTRYPOINT", _*) => true
    case ExecCmd("CMD", _*) => true
    case _ => false
  } ++ Seq(
    ExecCmd("ADD", "etc", "/etc"),
    ExecCmd("RUN", "chmod", "a+x", s"/etc/service/${name.value}/run"),
    ExecCmd("RUN", "chmod", "a+x", s"/etc/service/${name.value}/log"),
    ExecCmd("RUN", "chmod", "a+x", s"${(defaultLinuxInstallLocation in Docker).value}/bin/${(executableScriptName in Universal).value}"),
    ExecCmd("RUN", "chmod", "-R", "a+rX", "."),
    Cmd("LABEL", s"com.socrata.${name.value}=")
  )
)

lazy val balboa = (project in file(".")).
  aggregate(admin, agent, kafkaClient, jmsClient, http)

lazy val common = (project in file("balboa-common"))
  .enablePlugins(sbtbuildinfo.BuildInfoPlugin).
  settings(commonSettings: _*).
  settings(ivySettings: _*).
  settings(
    name := "balboa-common",
    libraryDependencies ++= Seq(
      junit,
      protobuf_java,
      mockito_test,
      jackson_core_asl,
      jackson_mapper_asl,
      jopt_simple
    ) ++ balboa_logging
  )

lazy val kafkaCommon = (project in file("balboa-common-kafka"))
  .dependsOn(common)
  .enablePlugins(sbtbuildinfo.BuildInfoPlugin).
  settings(commonSettings: _*).
  settings(ivySettings: _*).
  settings(
    name := "balboa-common-kafka",
    libraryDependencies ++= Seq(
      kafka,
      kafka_test
    )
  )

lazy val core = (project in file("balboa-core"))
  .dependsOn(common)
  .enablePlugins(sbtbuildinfo.BuildInfoPlugin).
  settings(commonSettings: _*).
  settings(ivySettings: _*).
  settings(
    name := "balboa-core",
    libraryDependencies ++= Seq(
      junit,
      astyanax
    )
  )

lazy val coreClient = (project in file("balboa-client")).
  dependsOn(common % "test->test;compile->compile").
  enablePlugins(sbtbuildinfo.BuildInfoPlugin).
  settings(commonSettings: _*).
  settings(ivySettings: _*).
  settings(
    name := "balboa-client"
  )

lazy val coreService = (project in file("balboa-service-core")).
  dependsOn(core).
  enablePlugins(sbtbuildinfo.BuildInfoPlugin).
  settings(commonSettings: _*).
  settings(ivySettings: _*).
  settings(
    name := "balboa-service-core"
  )

// Balboa Admin is intended to be a command line application.
// Therefore this sub project utilizes SBT Native Packager to produce gz or tar command line application packages.
lazy val admin = (project in file("balboa-admin")).
  dependsOn(core).
  enablePlugins(sbtbuildinfo.BuildInfoPlugin, JavaAppPackaging, UniversalPlugin).
  settings(commonSettings: _*).
  settings(dockerSettings: _*).
  settings(
    name := "balboa-admin",
    libraryDependencies ++= Seq(
      junit,
      opencsv
    )
  )

lazy val agent = (project in file("balboa-agent")).
  dependsOn(common, jmsClient).
  enablePlugins(sbtbuildinfo.BuildInfoPlugin, JavaAppPackaging, UniversalPlugin, DebianPlugin, DockerPlugin).
  settings(commonSettings: _*).
  settings(dockerSettings: _*).
  settings(
    name := "balboa-agent",
    libraryDependencies ++= Seq(
      commons_logging,
      dropwizard_metrics,
      dropwizard_servlets,
      junit,
      mockito_test
    )
  )

lazy val jmsClient = (project in file("balboa-client-jms")).
  dependsOn(coreClient % "test->test;compile->compile").
  enablePlugins(sbtbuildinfo.BuildInfoPlugin).
  settings(commonSettings: _*).
  settings(ivySettings: _*).
  settings(
    name := "balboa-client-jms",
    libraryDependencies ++= Seq(
      activemq
    )
  )

lazy val kafkaClient = (project in file("balboa-client-kafka")).
  dependsOn(kafkaCommon % "test->test;compile->compile", coreClient % "test->test;compile->compile").
  enablePlugins(sbtbuildinfo.BuildInfoPlugin).
  settings(commonSettings: _*).
  settings(ivySettings: _*).
  settings(
    name := "balboa-client-kafka",
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v")
  )

lazy val http = (project in file("balboa-http")).
  dependsOn(core).
  enablePlugins(sbtbuildinfo.BuildInfoPlugin, JavaAppPackaging, UniversalPlugin, DockerPlugin).
  settings(commonSettings: _*).
  settings(dockerSettings: _*).
  settings(
    name := "balboa-http",
    scalaVersion := "2.10.6",
    libraryDependencies ++= Seq(
      junit,
      rojoma_json,
      simple_arm,
      socrata_http
    )
  )


//lazy val balboaAgent = (project in file("balboa-agent")).
//  enablePlugins(JavaAppPackaging, UniversalPlugin, DebianPlugin, DockerPlugin).
//  settings(commonSettings: _*).
//  settings(
//    name := "balboa-agent",
//    libraryDependencies ++= Seq(
//      junit,
//      opencsv
//    )
//  )