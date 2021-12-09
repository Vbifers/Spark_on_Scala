
name := "spark"

version := "0.1"

scalaVersion := "2.12.10"

//spark-packages
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2" % "compile"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2" % "compile"

Compile / mainClass := Some("CommonApp")
assembly / mainClass := Some("CommonApp")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first

}