name := "sprocrest"

scalaVersion := "2.11.4"
scalacOptions ++= Seq("-feature", "-deprecation")

resolvers += Resolver.url("Edulify Repository", url("http://edulify.github.io/modules/releases/"))(Resolver.ivyStylePatterns)

libraryDependencies ++= Seq(
  jdbc,
  "de.zalando" % "zalando-sprocwrapper" % "1.0.6" excludeAll(
    ExclusionRule(organization = "org.springframework"),
    ExclusionRule(organization = "com.jolbox"),
    ExclusionRule(organization = "org.postgresql"),
    ExclusionRule(organization = "org.hiberate"),
    ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12")
    ),
  "com.typesafe.slick" %% "slick" % "2.1.0",
  "com.typesafe.play" %% "play-slick" % "0.8.0",
  "org.postgresql" % "postgresql" % "9.3-1102-jdbc4",
  "com.edulify" %% "play-hikaricp" % "1.4.1",
  "com.gilt" %% "play-json-service-lib-2-3" % "1.0.0",
  "org.specs2" %% "specs2" % "2.3.11" % "test"
)

lazy val root = (project in file(".")).enablePlugins(PlayScala)

TwirlKeys.templateFormats += ("json" -> "com.gilt.play.json.templates.JsonFormat")

// scoverage

import scoverage.ScoverageSbtPlugin.ScoverageKeys._

coverageExcludedPackages := "<empty>;Reverse.*;views.json..*"
coverageMinimum := 100
coverageFailOnMinimum := true


