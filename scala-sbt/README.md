# scala-sbt
Build and run Scala example

	$ sbt clean assembly
	
	$ flink run --class FlinkKafkaExample \
		target/scala-2.11/flink-streaming-confluent-assembly-1.0.jar \
		--topic test_topic --env conf/example.properties --host localhost --port 9000