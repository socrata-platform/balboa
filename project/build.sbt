resolvers := Seq(
  "socrata maven" at "https://repository-socrata-oss.forge.cloudbees.com/release"
)

libraryDependencies ++= Seq(
  "com.rojoma" %% "simple-arm" % "1.2.0",
  "com.rojoma" %% "rojoma-json" % "2.4.3"
)
