resolvers := Seq(
  "socrata maven" at "https://repository-socrata-oss.forge.cloudbees.com/release"
)

addSbtPlugin("com.socrata" % "socrata-cloudbees-sbt" % "1.2.3")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.6.0")

libraryDependencies ++= Seq(
  "com.rojoma" %% "simple-arm" % "1.2.0",
  "com.rojoma" %% "rojoma-json" % "2.0.0"
)
