package org.flink.catalog.config

import com.typesafe.config.Config

import java.util.Properties
import scala.collection.JavaConverters._

trait HelperConfigs {

  implicit class configMapperOps(config: Config) {
    def toMap: Map[String, AnyRef] =
      config
        .entrySet()
        .asScala
        .map(pair => (pair.getKey, config.getAnyRef(pair.getKey)))
        .toMap

    def toProperties: Properties = {
      val properties = new Properties()
      config.toMap.foreach(value => {
        properties.setProperty(value._1, value._2.toString)
      })

      properties
    }
  }

}
