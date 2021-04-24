package org.flink.catalog.events

case class Record(
    name: Option[String] = None,
    age: Option[Int] = None,
    date: Option[String] = None,
    city: Option[String] = None
)
