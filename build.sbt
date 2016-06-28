//
// Cross compiling situation
//
// As things stand, the balboa-clients need to be cross compiled because they
// are published as libraries and consumed by both Scala 2.10 and Scala 2.11
// projects.
//
// Now, odd quirk of sbt, it takes its crossScalaVersions setting from the
// project context in which it is invoked, not the project context which is
// being built. So:
//
//     sbt +balboa-client-dispatcher/compile
//
// takes the crossScalaVersions value from the balboa project (because it is
// the root project), not from a setting in balboa-client-dispatcher.
//
//     sbt "project balboa-client-dispatcher" "+compile"
//
// takes the crossScalaVersions value from the balboa-client-dispatcher
// project.
//
// Because of this sbt oddity, if you are testing cross compiling for any
// projects, you have to take special action.
//
// And because we have a weird shared build script that attempts to accommodate
// all invocation paths, so needed to remain backwards compatible, the Jenkins
// build jobs work yet a different way, and are invoked:
//
//     sbt 'set crossScalaVersions := List("2.10.6", "2.11.8")' "+balboa-client-dispatcher/compile"
//

scalaVersion := "2.11.8"

maintainer := "Socrata Mission Control Team, mission-control-l@socrata.com"

packageSummary := "Balboa"

packageDescription :=
  """
    |Internal Metrics System.
  """.stripMargin
