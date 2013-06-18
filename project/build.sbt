resolvers := Seq(
  "socrata maven" at "https://repo.socrata.com/artifactory/libs-release",
  "socrata maven local" at "https://repo.socrata.com/artifactory/libs-release-local",
  "socrata repo1-cache" at "https://repo.socrata.com/artifactory/repo1-cache",
  Resolver.url("socrata ivy", new URL("https://repo.socrata.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns)
)

externalResolvers <<= resolvers map { rs =>
  Resolver.withDefaultResolvers(rs, mavenCentral = false)
}

addSbtPlugin("com.socrata" % "socrata-sbt" % "0.3.1")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.6.0")
