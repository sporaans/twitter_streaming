
name := "test-project"
organization := "com.test"
scalaVersion := "2.12.10"
version := "1"


val sparkVersion = "3.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0",
  "org.apache.kafka" % "kafka_2.12" % "3.2.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.2.0"
)


libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.12" % "test")


assemblyMergeStrategy in assembly := {
  case "META-INF/services/java.sql.Driver" => MergeStrategy.concat
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false, includeDependency = false)