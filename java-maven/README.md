# java-maven
Build and run Java example

	$ mvn clean package
	
	$ flink run --class FlinkKafkaExample \
		target/flink-streaming-confluent-1.0-SNAPSHOT-all.jar \
		--topic test_topic --env conf/example.properties --host localhost --port 9000