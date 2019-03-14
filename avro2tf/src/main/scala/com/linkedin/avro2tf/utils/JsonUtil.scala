package com.linkedin.avro2tf.utils

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/**
 * Helper class to serialize and deserialize between JSON String and map
 */
object JsonUtil {

  /**
   * Define the FasterXML Jackson object mapper
   */
  private[this] val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper
    .registerModule(DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
    .configure(SerializationFeature.INDENT_OUTPUT, true)

  /**
   * Write an object to JSON formatted String
   *
   * @param value Any object to be written
   * @return A JSON pretty formatted String
   */
  def toJsonString(value: Any): String = mapper.writeValueAsString(value)
}