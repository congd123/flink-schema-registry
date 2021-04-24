package org.flink.catalog.serdes

import org.apache.avro.SchemaBuilder

object GenericRecordSchema {
  val schema = SchemaBuilder
    .record("record")
    .fields()
    .nullableString("name", "--empty--")
    .nullableInt("age", 0)
    .nullableString("date", "--empty--")
    .nullableString("city", "--empty--")
    .endRecord()
}
