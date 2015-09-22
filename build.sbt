/////////////
// Imports //
/////////////
import uk.gov.hmrc.GitStampPlugin._

import S3._

//////////////////////////////
// Project Meta Information //
//////////////////////////////
organization  := "de.frosner"

version       := "3.1.0-SNAPSHOT"

name          := "spawncamping-dds"

scalaVersion  := "2.10.5"

lazy val shortScalaVersion = settingKey[String]("Scala major and minor version.")

shortScalaVersion := scalaVersion.value.split("\\.").take(2).mkString(".")

lazy val finalArtifactName = settingKey[String]("Name of the final artifact.")

finalArtifactName := s"${name.value}-${version.value}_${shortScalaVersion.value}.jar"

//////////////////////////
// Library Dependencies //
//////////////////////////
libraryDependencies ++= {
  val sprayV = "1.3.2"
  val sparkV = "1.4.0"
  Seq(
    "io.spray"            %% "spray-can"                   % sprayV,
    "io.spray"            %% "spray-routing"               % sprayV,
    "io.spray"            %% "spray-caching"               % sprayV,
    "io.spray"            %% "spray-json"                  % "1.3.1",
    "com.typesafe.akka"   %% "akka-actor"                  % "2.3.6",
    "org.scalaj"          %% "scalaj-http"                 % "1.1.4",
    "org.scalaz"          %% "scalaz-core"                 % "7.1.3",
    "org.slf4j"           %  "slf4j-log4j12"               % "1.7.10",
    "org.apache.spark"    %% "spark-core"                  % sparkV     % "provided",
    "org.apache.spark"    %% "spark-graphx"                % sparkV     % "provided",
    "org.apache.spark"    %% "spark-sql"                   % sparkV     % "provided",
    "org.scalatest"       %% "scalatest"                   % "2.2.4"    % "test",
    "org.scalamock"       %% "scalamock-scalatest-support" % "3.2.1"    % "test"
  )
}

/////////////////////
// Compile Options //
/////////////////////
fork in Compile := true

crossScalaVersions := Seq("2.11.6", scalaVersion.value)

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

//////////////////
// Test Options //
//////////////////
Seq(jasmineSettings: _*)

(test in Test) <<= (test in Test) dependsOn (jasmine)

appJsDir <+= sourceDirectory { src => src / "main" / "resources" / "ui" / "app" }

appJsLibDir <+= sourceDirectory {  src => src / "main" / "resources" / "ui" / "lib" }

jasmineTestDir <+= sourceDirectory { src => src / "test" / "resources" / "ui" }

jasmineConfFile <+= sourceDirectory { src => src / "test" / "resources" / "ui" / "test.dependencies.js" }

jasmineRequireJsFile <+= sourceDirectory { src => src / "main" / "resources" / "ui" / "lib" / "require.js" }

//////////////////////
// Assembly Options //
//////////////////////
Seq(gitStampSettings: _*)

test in assembly := {}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := finalArtifactName.value

///////////////////////
// Custom Build Task //
///////////////////////
lazy val build = taskKey[Unit]("Jarjar link the assembly jar!")

build <<= assembly map { (asm) => s"./build.sh ${asm.getAbsolutePath()}" ! }

/////////////////////////////////
// Custom Artficat Upload Task //
/////////////////////////////////
lazy val currentBranch = System.getenv("TRAVIS_BRANCH")

val isSnapshotBranch = settingKey[Boolean]("Snapshot branch is active")

isSnapshotBranch := (currentBranch != null) && (currentBranch == "master" || currentBranch.startsWith("release/"))

val dontPublishTask = TaskKey[Unit]("dont-publish-to-s3", "Don't publish branch SNAPSHOT to S3.")

dontPublishTask <<= (streams) map { (s) => {
    s.log.info(s"""Not publishing artifact to S3 (on branch $currentBranch)""")
  }
}

val publishOrDontPublishTask = TaskKey[Unit]("publish-snapshot", "Publish depending on the current branch.")

publishOrDontPublishTask := Def.taskDyn({
  if(isSnapshotBranch.value) S3.upload.toTask
  else dontPublishTask.toTask
}).value

s3Settings

mappings in upload := Seq((new java.io.File(s"${System.getProperty("user.dir")}/target/scala-${shortScalaVersion.value}/${finalArtifactName.value}"),finalArtifactName.value))

host in upload := "spawncamping-dds-snapshots.s3.amazonaws.com"

credentials += Credentials("Amazon S3", "spawncamping-dds-snapshots.s3.amazonaws.com", System.getenv("ARTIFACTS_KEY"), System.getenv("ARTIFACTS_SECRET"))
