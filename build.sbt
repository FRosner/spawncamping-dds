/////////////
// Imports //
/////////////
import uk.gov.hmrc.GitStampPlugin._

import S3._

import Dependencies._

////////////////////////////////////////////////
// Common Settings Shared Across Sub-Projects //
////////////////////////////////////////////////
lazy val rootProjectName = settingKey[String]("Name of the root project")

lazy val commonMetaInformationSettings = Seq(
  organization      := "de.frosner",
  version           := "4.0.0-alpha",
  scalaVersion      := "2.10.6",
  rootProjectName   := "spawncamping-dds"
)

lazy val commonCompileSettings = Seq(
  fork in Compile := true,
  scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8"),
  resolvers += "jitpack" at "https://jitpack.io"
)

lazy val commonSettings = commonMetaInformationSettings ++ commonCompileSettings

commonSettings

////////////////////////////////
// Root Project Only Settings //
////////////////////////////////
lazy val shortScalaVersion = settingKey[String]("Scala major and minor version.")

lazy val finalArtifactName = settingKey[String]("Name of the final artifact.")

lazy val rootMetaInformationSettings = Seq(
  name := rootProjectName.value
)

lazy val rootAssemblySettings = Seq(gitStampSettings:_*) ++ Seq(
  shortScalaVersion := scalaVersion.value.split("\\.").take(2).mkString("."),
  finalArtifactName := s"${name.value}-${version.value}_${shortScalaVersion.value}.jar",
  test in assembly := {},
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
  assemblyJarName in assembly := finalArtifactName.value
)

lazy val rootSettings = rootMetaInformationSettings ++ rootAssemblySettings

rootSettings // TODO check where we need these settings and if it makes sense to include them into common settings?

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

///////////////////////
// Project Structure //
///////////////////////
lazy val root = (project in file(".")).
  settings((commonSettings ++ rootSettings): _*).
  settings(
    name := rootProjectName.value
  ).
  aggregate(core, datasets, webUi).
  dependsOn(core, datasets, webUi)

lazy val core = (project in file("core")).
  settings((commonSettings ++ rootSettings): _*).
  settings(
    name := rootProjectName.value + "-core",
    libraryDependencies ++= coreDependencies
  )

lazy val datasets = (project in file("datasets")).
  settings((commonSettings ++ rootSettings): _*).
  settings(
    name := rootProjectName.value + "-datasets",
    libraryDependencies ++= datasetsDependencies
  )

lazy val webUi = (project in file("web-ui")).
  dependsOn(core).
  settings((commonSettings ++ rootSettings): _*).
  settings(
    name := rootProjectName.value + "-web-ui",
    libraryDependencies ++= webUiDependencies
  )
