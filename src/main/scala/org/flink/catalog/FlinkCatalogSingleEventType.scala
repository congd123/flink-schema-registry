package org.flink.catalog

import com.catalog.avro.{Record, User}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.formats.avro.registry.confluent.{
  ConfluentRegistryAvroDeserializationSchema,
  ConfluentRegistryAvroSerializationSchema
}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.flink.catalog.config.HelperConfigs
import org.flink.catalog.config.KafkaConfigs.{ConsumerConfig, ProducerConfig, SchemaRegistryConfig}
import pureconfig.ConfigSource

object FlinkCatalogSingleEventType extends App with HelperConfigs {

  val configFile = sys.props
    .get("config.file")
    .getOrElse(getClass.getResource("/kafka-configs.conf").getPath)

  val consumerConfigs = ConfigSource.file(configFile).loadOrThrow[ConsumerConfig].consumerConfig
  val producerConfigs = ConfigSource.file(configFile).loadOrThrow[ProducerConfig].producerConfig
  val schemaRegistryConfigs = ConfigSource.file(configFile).loadOrThrow[SchemaRegistryConfig].schemaRegistryConfig

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val input: DataStream[Record] = env.addSource(
    new FlinkKafkaConsumer[Record](
      consumerConfigs.getString("topic"),
      ConfluentRegistryAvroDeserializationSchema
        .forSpecific(classOf[Record], schemaRegistryConfigs.getString("schema.registry.url")),
      consumerConfigs.toProperties
    )
  )
  val avroFlinkKafkaProducer = new FlinkKafkaProducer[User](
    producerConfigs.getString("topic"),
    ConfluentRegistryAvroSerializationSchema
      .forSpecific(classOf[User], "user-value", schemaRegistryConfigs.getString("schema.registry.url")),
    producerConfigs.toProperties
  )

  input
    .map(new RichMapFunction[User, User] {
      override def map(user: User): User = {
        val name = s"${user.getName} test"
        user.setName(name)

        user
      }
    })
    .addSink(avroFlinkKafkaProducer)

  env.execute("Flink using Schema Registry")
}
