resolvers := Seq(
  "socrata maven" at "https://repository-socrata-oss.forge.cloudbees.com/release"
)

addSbtPlugin("com.socrata" % "socrata-sbt-plugins" % "1.6.1")
// Sbt Native Packager
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.0.4")
