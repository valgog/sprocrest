logLevel := Level.Warn

resolvers += Classpaths.sbtPluginReleases
resolvers += Classpaths.typesafeReleases

addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.3.6")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.0.1")
