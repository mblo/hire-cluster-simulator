// Project settings
name := "HIRE simulator"
scalaVersion := "2.13.4"
scalacOptions += "-target:jvm-11"
scalacOptions ++= Seq(
  "-deprecation", // emit warning and location for usages of deprecated APIs
)

javaOptions in run += "-Xmx15G"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % "test"
logBuffered in Test := false
parallelExecution in Test := false
test in assembly := {}
mainClass in assembly := Some("hiresim.experiments.SimRunnerFromCmdArguments")
assemblyOutputPath in assembly := new File("target/hire.jar")

