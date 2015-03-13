organization  := "de.frosner"

version       := "1.2.0-SNAPSHOT"

name          := "spawncamping-dds"

scalaVersion  := "2.11.2"

crossScalaVersions := Seq("2.10.4", scalaVersion.value)

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

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.0" % "provided"

libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "1.1.4"

libraryDependencies +=  "org.scalamock" %% "scalamock-scalatest-support" % "3.2.1" % "test"

test in assembly := {}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := name.value + "-" + version.value + "_" + scalaVersion.value + ".jar"

fork in Compile := true

lazy val build = taskKey[Unit]("Jarjar link the assembly jar!")

build <<= assembly map { (asm) => s"./build.sh ${asm.getAbsolutePath()}" ! }
