addSbtPlugin("com.socrata" % "socrata-sbt" % "0.2.3")

libraryDependencies <+= sbtVersion(v => "com.github.siasia" %% "xsbt-web-plugin" % (v+"-0.2.11.1"))
