resolvers := Seq(
  "socrata maven" at "https://repo.socrata.com/artifactory/libs-release",
  Resolver.url("socrata ivy", new URL("https://repo.socrata.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns)
)

externalResolvers <<= resolvers map { rs =>
  Resolver.withDefaultResolvers(rs, mavenCentral = false)
}

addSbtPlugin("com.socrata" % "socrata-sbt" % "0.2.3")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.6.0")

libraryDependencies <+= sbtVersion(v => "com.github.siasia" %% "xsbt-web-plugin" % (v+"-0.2.11.1"))
