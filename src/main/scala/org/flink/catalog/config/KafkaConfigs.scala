package org.flink.catalog.config

import com.typesafe.config.Config

object KafkaConfigs {
  case class SchemaRegistryConfig(schemaRegistryConfig: Config)

  case class ConsumerConfig(consumerConfig: Config)

  case class ProducerConfig(producerConfig: Config)
}
