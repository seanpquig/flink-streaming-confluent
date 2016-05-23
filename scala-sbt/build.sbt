name := "flink-streaming-confluent"

version := "1.0"

scalaVersion := "2.11.7"

val flinkVersion = "1.0.3"

resolvers += "Confluent Maven Repo" at "http://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  "io.confluent" % "kafka-avro-serializer" % "2.0.1",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-connector-kafka-0.8" % flinkVersion
)
