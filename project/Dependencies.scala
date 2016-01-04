import sbt._

object Dependencies {

  // Versions
  lazy val sparkVersion = "1.5.0"
  lazy val sprayVersion = "1.3.2"
  lazy val replHelperVersion = "1.0.2"

  // Dependencies
  val sparkDependencies = Seq(
    "org.apache.spark" %% "spark-core"    % sparkVersion % "provided",
    "org.apache.spark" %% "spark-graphx"  % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql"     % sparkVersion % "provided"
  )
  val scalaTestDependencies = Seq(
    "org.scalatest" %% "scalatest"                   % "2.2.4" % "test",
    "org.scalamock" %% "scalamock-scalatest-support" % "3.2.1" % "test"
  )

  // Projects
  val coreDependencies = sparkDependencies ++ scalaTestDependencies ++ Seq(
    "org.scalaz"          %% "scalaz-core" % "7.1.3",
    "com.github.FRosner"  %% "repl-helper" % replHelperVersion
  )
  val datasetsDependencies = sparkDependencies ++ scalaTestDependencies
  val webUiDependencies = scalaTestDependencies ++ sparkDependencies ++ Seq(
    "io.spray"           %% "spray-can"     % sprayVersion,
    "io.spray"           %% "spray-routing" % sprayVersion,
    "io.spray"           %% "spray-caching" % sprayVersion,
    "io.spray"           %% "spray-json"    % "1.3.1",
    "com.typesafe.akka"  %% "akka-actor"    % "2.3.6",
    "org.scalaj"         %% "scalaj-http"   % "1.1.4",
    "com.github.FRosner" %% "repl-helper"   % replHelperVersion
  )

}
