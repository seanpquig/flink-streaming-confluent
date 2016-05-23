import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.ConfluentAvroDeserializationSchema;

import java.util.Properties;

public final class FlinkKafkaExample {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get CLI parameters
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String topic = parameters.getRequired("topic");
        String groupId = parameters.get("group-id", "flink-kafka-consumer");
        String propertiesFile = parameters.getRequired("env");
        ParameterTool envProperties = ParameterTool.fromPropertiesFile(propertiesFile);
        String schemaRegistryUrl = envProperties.getRequired("registry_url");
        String bootstrapServers = envProperties.getRequired("brokers");
        String zookeeperConnect = envProperties.getRequired("zookeeper");

        // setup Kafka sink
        ConfluentAvroDeserializationSchema deserSchema = new ConfluentAvroDeserializationSchema(schemaRegistryUrl);
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", bootstrapServers);
        kafkaProps.setProperty("zookeeper.connect", zookeeperConnect);
        kafkaProps.setProperty("group.id", groupId);
        FlinkKafkaConsumer08<String> flinkKafkaConsumer = new FlinkKafkaConsumer08<String>(topic, deserSchema, kafkaProps);

        DataStream<String> kafkaStream = env.addSource(flinkKafkaConsumer);

        DataStream<Integer> counts = kafkaStream
                .map(new MapFunction<String, Integer>() {
                    public Integer map(String s) throws Exception {
                        return 1;
                    }
                })
                .timeWindowAll(Time.seconds(3))
                .sum(0);

        counts.print();

        env.execute("Flink Kafka Java Example");
    }

}
