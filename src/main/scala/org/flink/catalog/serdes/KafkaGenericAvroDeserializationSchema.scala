package org.flink.catalog.serdes

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.flink.catalog.events.Record

class KafkaGenericAvroDeserializationSchema(deserializer: KafkaAvroDeserializer, topic: String)
    extends KafkaDeserializationSchema[Record] {

  override def isEndOfStream(t: Record): Boolean = false

  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): Record = {
    val record = deserializer.deserialize(topic, consumerRecord.value()).asInstanceOf[GenericRecord]
    record.getSchema.getName match {
      case "TypeA" =>
        Record(name = Some(record.get("name").toString))
      case "TypeB" =>
        Record(
          age = Some(record.get("age").asInstanceOf[Int]),
          date = Some(record.get("date").toString),
          city = Some(record.get("city").toString)
        )
      case _ => null
    }
  }

  override def getProducedType: TypeInformation[Record] = {
    TypeExtractor.getForClass(classOf[Record]);
  }
}
