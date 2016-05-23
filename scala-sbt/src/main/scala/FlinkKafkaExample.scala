import java.util.Properties

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.ConfluentAvroDeserializationSchema

object FlinkKafkaExample extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // get CLI parameters
  val parameters = ParameterTool.fromArgs(args)
  val topic = parameters.getRequired("topic")
  val groupId = parameters.get("group-id", "flink-kafka-consumer")
  val propertiesFile = parameters.getRequired("env")
  val envProperties = ParameterTool.fromPropertiesFile(propertiesFile)
  val schemaRegistryUrl = envProperties.getRequired("registry_url")
  val boostrapServers = envProperties.getRequired("brokers")
  val zookeeperConnect = envProperties.getRequired("zookeeper")

  // setup Kafka sink
  val deserSchema = new ConfluentAvroDeserializationSchema(schemaRegistryUrl)
  val kafkaProps = new Properties()
  kafkaProps.setProperty("bootstrap.servers", boostrapServers)
  kafkaProps.setProperty("zookeeper.connect", zookeeperConnect)
  kafkaProps.setProperty("group.id", groupId)
  val flinkKafkaConsumer = new FlinkKafkaConsumer08[String](topic, deserSchema, kafkaProps)

  val kafkaStream = env.addSource(flinkKafkaConsumer)

  val counts = kafkaStream
    .map { _ => 1 }
    .timeWindowAll(Time.seconds(3))
    .sum(0)

  counts.print

  env.execute("Flink Kafka Scala Example")

}
