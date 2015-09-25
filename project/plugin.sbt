addSbtPlugin("com.typesafe.sbt" % "sbt-s3" % "0.8")

resolvers += Resolver.url("hmrc-sbt-plugin-releases",
  url("https://dl.bintray.com/hmrc/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

addSbtPlugin("com.joescii" % "sbt-jasmine-plugin" % "1.3.0")

addSbtPlugin("uk.gov.hmrc" % "sbt-git-stamp" % "4.5.0")

resolvers += Classpaths.sbtPluginReleases

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.0.4")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.0.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.1")

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
