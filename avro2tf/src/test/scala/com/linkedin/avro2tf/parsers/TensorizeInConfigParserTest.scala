package com.linkedin.avro2tf.parsers

import scala.io.Source

import com.linkedin.avro2tf.configs.{Combiner, HashInfo, Tokenization}
import com.linkedin.avro2tf.utils.ConstantsForTest._
import com.typesafe.config.ConfigException
import io.circe.generic.extras.auto._
import io.circe.parser.decode
import org.testng.Assert._
import org.testng.annotations.Test

class TensorizeInConfigParserTest {
  @Test
  def testJsonConfigLoading(): Unit = {

    val configString = loadConfigString(TENSORIZEIN_CONFIG_PATH_VALUE_2)
    val config = TensorizeInConfigParser.getTensorizeInConfiguration(configString)
    assertNotNull(config)
  }

  @Test
  def testJsonAndHoconEqual(): Unit = {

    val jsonConfigString = loadConfigString(TENSORIZEIN_CONFIG_PATH_VALUE_2)
    val hoconConfigString = loadConfigString(TENSORIZEIN_CONFIG_PATH_VALUE_2_HOCON)

    val jsonConfig = TensorizeInConfigParser.getTensorizeInConfiguration(jsonConfigString)
    val hoconConfig = TensorizeInConfigParser.getTensorizeInConfiguration(hoconConfigString)

    assertEquals(jsonConfig, hoconConfig)

    // test the hashcodes are equal too
    assertEquals(
      jsonConfig.features.head.outputTensorInfo.hashCode(),
      hoconConfig.features.head.outputTensorInfo.hashCode())
  }

  @Test(expectedExceptions = Array(classOf[ConfigException.Parse]))
  def testBadConfiguration(): Unit = {

    TensorizeInConfigParser.getTensorizeInConfiguration("bad string")
  }

  @Test
  def testHashInfoConfig(): Unit = {
    import com.linkedin.avro2tf.configs.JsonCodecs._

    val inputJson =
      """{
        |  "hashBucketSize": 1000,
        |  "numHashFunctions": 4
        |  }
        |""".stripMargin

    val expected = HashInfo(1000, 4, Combiner.SUM)

    val result = decode[HashInfo](inputJson)

    assertTrue(result.isRight)
    assertEquals(result.right.get, expected)
  }

  @Test
  def testEmptyTokenization(): Unit = {
    import com.linkedin.avro2tf.configs.JsonCodecs._

    // this is valid input for the tokenization field, as goofy as it looks
    val inputJson =
      """{}"""

    val expected = Tokenization()

    val result = decode[Tokenization](inputJson)

    assertTrue(result.isRight)
    assertEquals(result.right.get, expected)
  }

  def loadConfigString(path: String): String = {

    val resourceStream = getClass.getClassLoader.getResourceAsStream(path)
    val lines = Source.fromInputStream(resourceStream).getLines.mkString("\n")
    lines
  }
}