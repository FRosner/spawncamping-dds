import S3._

organization  := "de.frosner"

version       := "2.1.0-SNAPSHOT"

name          := "spawncamping-dds"

scalaVersion  := "2.10.5"

lazy val shortScalaVersion = settingKey[String]("Scala major and minor version.")

shortScalaVersion := scalaVersion.value.split("\\.").take(2).mkString(".")

lazy val currentBranch = ("git status -sb" !!).split("\\n")(0).stripPrefix("## ")

val isMasterBranch = settingKey[Boolean]("currentBranch is master")

isMasterBranch := currentBranch == "master" || System.getenv("TRAVIS_BRANCH") == "master"

lazy val finalArtifactName = settingKey[String]("Name of the final artifact.")

finalArtifactName := s"${name.value}-${version.value}_${shortScalaVersion.value}.jar"

crossScalaVersions := Seq("2.11.6", scalaVersion.value)

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= {
  val akkaV = "2.3.6"
  val sprayV = "1.3.2"
  Seq(
    "io.spray"            %%  "spray-can"     % sprayV,
    "io.spray"            %%  "spray-routing" % sprayV,
    "io.spray"            %%  "spray-caching" % sprayV,
    "io.spray"            %%  "spray-json"    % "1.3.1",
    "com.typesafe.akka"   %%  "akka-actor"    % akkaV
  )
}

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.1.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.3.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.3.0" % "provided"

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.10"

libraryDependencies += "org.scalaj" %% "scalaj-http" % "1.1.4"

libraryDependencies += "org.scalamock" %% "scalamock-scalatest-support" % "3.2.1" % "test"

test in assembly := {}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := finalArtifactName.value

val dontPublishTask = TaskKey[Unit]("dont-publish-to-s3", "Don't publish branch SNAPSHOT to S3.")

dontPublishTask <<= (streams) map { (s) => {
    s.log.info(s"""Not publishing artifact to S3 (on branch $currentBranch / ${System.getenv("TRAVIS_BRANCH")})""")
  }
}

val publishOrDontPublishTask = TaskKey[Unit]("publish-master-snapshot", "Publish depending on the current branch.")

publishOrDontPublishTask := Def.taskDyn({
  if(isMasterBranch.value) S3.upload.toTask
  else dontPublishTask.toTask
}).value

s3Settings

mappings in upload := Seq((new java.io.File(s"${System.getProperty("user.dir")}/target/scala-${shortScalaVersion.value}/${finalArtifactName.value}"),finalArtifactName.value))

host in upload := "spawncamping-dds-snapshots.s3.amazonaws.com"

credentials += Credentials("Amazon S3", "spawncamping-dds-snapshots.s3.amazonaws.com", System.getenv("ARTIFACTS_KEY"), System.getenv("ARTIFACTS_SECRET"))

fork in Compile := true

lazy val build = taskKey[Unit]("Jarjar link the assembly jar!")

build <<= assembly map { (asm) => s"./build.sh ${asm.getAbsolutePath()}" ! }
