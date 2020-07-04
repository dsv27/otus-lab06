name := "otus-lab06"

version := "0.1"

scalaVersion := "2.11.12"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.4",
  "org.apache.spark" %% "spark-sql" % "2.3.4",
  "org.json4s" %% "json4s-native" % "3.6.9",
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
