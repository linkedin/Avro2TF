package com.linkedin.avro2tf.parsers

import com.typesafe.config.ConfigException

import scala.io.Source
import org.testng.Assert._
import org.testng.annotations.Test

import com.linkedin.avro2tf.utils.ConstantsForTest._

class TensorizeInConfigParserTest {
  @Test
  def testJsonConfigLoading(): Unit = {
    val configString = loadConfigString(TENSORIZEIN_CONFIG_PATH_VALUE_2)
    val config = TensorizeInConfigParser.getTensorizeInConfiguration(configString)
  }

  @Test
  def testJsonAndHoconEqual(): Unit = {
    val jsonConfigString = loadConfigString(TENSORIZEIN_CONFIG_PATH_VALUE_2)
    val hoconConfigString = loadConfigString(TENSORIZEIN_CONFIG_PATH_VALUE_2_HOCON)

    val jsonConfig = TensorizeInConfigParser.getTensorizeInConfiguration(jsonConfigString)
    val hoconConfig = TensorizeInConfigParser.getTensorizeInConfiguration(hoconConfigString)

    assertEquals(jsonConfig, hoconConfig)

    // test the hashcodes are equal too
    assertEquals(jsonConfig.features.head.outputTensorInfo.hashCode(),
      hoconConfig.features.head.outputTensorInfo.hashCode())
  }

  @Test(expectedExceptions = Array(classOf[ConfigException.Parse]))
  def testBadConfiguration(): Unit = {
   TensorizeInConfigParser.getTensorizeInConfiguration("bad string")
  }

  def loadConfigString(path: String): String = {
    val resourceStream = getClass.getClassLoader.getResourceAsStream(path)
    val lines = Source.fromInputStream(resourceStream).getLines.mkString("\n")
    lines
  }
}