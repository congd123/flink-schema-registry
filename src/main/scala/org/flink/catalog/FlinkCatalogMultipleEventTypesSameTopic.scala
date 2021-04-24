package org.flink.catalog

import com.catalog.avro.User
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer, KafkaAvroDeserializerConfig}
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.flink.catalog.config.HelperConfigs
import org.flink.catalog.config.KafkaConfigs.{ConsumerConfig, ProducerConfig, SchemaRegistryConfig}
import org.flink.catalog.events.Record
import org.flink.catalog.serdes.KafkaGenericAvroDeserializationSchema
import pureconfig.ConfigSource

import scala.collection.JavaConverters.mapAsJavaMapConverter

object FlinkCatalogMultipleEventTypesSameTopic extends App with HelperConfigs {

  val configFile = sys.props
    .get("config.file")
    .getOrElse(getClass.getResource("/kafka-configs.conf").getPath)

  val consumerConfigs = ConfigSource.file(configFile).loadOrThrow[ConsumerConfig].consumerConfig
  val producerConfigs = ConfigSource.file(configFile).loadOrThrow[ProducerConfig].producerConfig
  val schemaRegistryConfigs = ConfigSource.file(configFile).loadOrThrow[SchemaRegistryConfig].schemaRegistryConfig

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.getConfig.enableForceAvro()
  env.getConfig.disableForceKryo()

  val registryUrl = schemaRegistryConfigs.getString("schema.registry.url")

  val props = Map(
    AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> registryUrl,
    KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG -> false
  )

  val schemaRegistryClient: SchemaRegistryClient =
    new CachedSchemaRegistryClient(registryUrl, AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT);
  val deserializer = new KafkaAvroDeserializer(schemaRegistryClient, props.asJava)

  val input = env.addSource(
    new FlinkKafkaConsumer[Record](
      consumerConfigs.getString("topic"),
      new KafkaGenericAvroDeserializationSchema(
        deserializer,
        consumerConfigs.getString("topic")
      ),
      consumerConfigs.toProperties
    ).setStartFromEarliest()
  )

  val avroFlinkKafkaProducer = new FlinkKafkaProducer[User](
    producerConfigs.getString("topic"),
    ConfluentRegistryAvroSerializationSchema
      .forSpecific(classOf[User], "user-value", registryUrl),
    producerConfigs.toProperties
  )

  input
    .map((record: Record) => {
      println(record.toString)

      // TODO: Your business logic ...

      User
        .newBuilder()
        .setName(record.name.getOrElse("--unkown--"))
        .build()
    })
    .addSink(avroFlinkKafkaProducer)

  env.execute("Flink using Schema Registry")
}
