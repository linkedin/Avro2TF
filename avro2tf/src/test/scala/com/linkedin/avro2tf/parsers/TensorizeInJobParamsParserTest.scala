package com.linkedin.avro2tf.parsers

import java.io.File

import scala.collection.mutable
import com.linkedin.avro2tf.configs.{Feature, InputFeatureInfo, OutputTensorInfo, TensorizeInConfiguration}
import com.linkedin.avro2tf.utils.Constants
import com.linkedin.avro2tf.utils.ConstantsForTest._
import org.testng.Assert._
import org.testng.annotations.Test

/**
 * Test the parser file for TensorizeIn job parameters from command line arguments
 *
 */
class TensorizeInJobParamsParserTest {

  /**
   * Test if the command line argument parser can work properly when well-formed parameters are specified
   *
   */
  @Test
  def testParse(): Unit = {

    // Get expected TensorizeIn configuration and parameters
    val expectedTensorizeInConfig = getExpectedTensorizeInConfig
    val expectedTensorizeInParams = getExpectedTensorizeInParams(expectedTensorizeInConfig)

    // Get actual TensorizeIn configuration and parameters
    val tensorizeInConfigPath = new File(
      getClass.getClassLoader.getResource(TENSORIZEIN_CONFIG_PATH_VALUE_2).getFile
    ).getAbsolutePath
    val arguments = getCommandLineArguments(tensorizeInConfigPath)
    val actualTensorizeInParams = TensorizeInJobParamsParser.parse(arguments)

    // Test assert equals on TensorizeIn parameters without testing TensorizeIn configuration
    testParamsEqualsWithoutTensorizeInConfig(actualTensorizeInParams, expectedTensorizeInParams)

    // Test assert equals on a sample of TensorizeIn configuration
    testTensorizeInConfigEquals(actualTensorizeInParams, expectedTensorizeInParams)
  }

  /**
   * Test if the command line argument parser can work properly when a default column expression need to be added
   *
   */
  @Test
  def testParseWithDefaultColExpr(): Unit = {

    // Get expected TensorizeIn configuration and parameters
    val expectedTensorizeInConfig = getExpectedTensorizeInConfig
    val expectedTensorizeInParams = getExpectedTensorizeInParams(expectedTensorizeInConfig)

    // Get actual TensorizeIn configuration and parameters
    val tensorizeInConfigPath = new File(
      getClass.getClassLoader.getResource(TENSORIZEIN_CONFIG_PATH_VALUE_3).getFile
    ).getAbsolutePath
    val arguments = getCommandLineArguments(tensorizeInConfigPath)
    val actualTensorizeInParams = TensorizeInJobParamsParser.parse(arguments)

    // Test assert equals on TensorizeIn parameters without testing TensorizeIn configuration
    testParamsEqualsWithoutTensorizeInConfig(actualTensorizeInParams, expectedTensorizeInParams)

    // Test assert equals on a sample of TensorizeIn configuration
    testTensorizeInConfigEquals(actualTensorizeInParams, expectedTensorizeInParams)
  }

  /**
   * Test if the command line argument parser can throw exception when both column expression and column configuration exist
   *
   */
  @Test(expectedExceptions = Array(classOf[IllegalArgumentException]))
  def testParseWithBothColExprAndColConfig(): Unit = {

    // Get actual TensorizeIn configuration and parameters
    val tensorizeInConfigPath = new File(
      getClass.getClassLoader.getResource(TENSORIZEIN_CONFIG_PATH_VALUE_4).getFile
    ).getAbsolutePath
    val arguments = getCommandLineArguments(tensorizeInConfigPath)

    TensorizeInJobParamsParser.parse(arguments)
  }

  /**
   * Test if the command line argument parser can work properly when a default shape need to be added
   *
   */
  @Test
  def testParseWithDefaultShape(): Unit = {

    // Get expected TensorizeIn configuration and parameters
    val expectedTensorizeInConfig = getExpectedTensorizeInConfig
    val expectedTensorizeInParams = getExpectedTensorizeInParams(expectedTensorizeInConfig)

    // Get actual TensorizeIn configuration and parameters
    val tensorizeInConfigPath = new File(
      getClass.getClassLoader.getResource(TENSORIZEIN_CONFIG_PATH_VALUE_5).getFile
    ).getAbsolutePath
    val arguments = getCommandLineArguments(tensorizeInConfigPath)
    val actualTensorizeInParams = TensorizeInJobParamsParser.parse(arguments)

    // Test assert equals on TensorizeIn parameters without testing TensorizeIn configuration
    testParamsEqualsWithoutTensorizeInConfig(actualTensorizeInParams, expectedTensorizeInParams)

    // Test assert equals on a sample of TensorizeIn configuration
    testTensorizeInConfigEquals(actualTensorizeInParams, expectedTensorizeInParams)
  }

  /**
   * Test if the command line argument parser can throw exception when both input date and days range exist
   *
   */
  @Test(expectedExceptions = Array(classOf[IllegalArgumentException]))
  def testParseWithBothDateAndDaysRange(): Unit = {

    // Get actual TensorizeIn configuration and parameters
    val tensorizeInConfigPath = new File(
      getClass.getClassLoader.getResource(TENSORIZEIN_CONFIG_PATH_VALUE_2).getFile
    ).getAbsolutePath
    val arguments = getCommandLineArguments(tensorizeInConfigPath) ++ Seq(INPUT_DAYS_RANGE_NAME, INPUT_DAYS_RANGE_VALUE)

    TensorizeInJobParamsParser.parse(arguments)
  }

  /**
   * Test that the parsing fails when incorrect execution mode is passed
   *
   */
  @Test(expectedExceptions = Array(classOf[IllegalArgumentException]))
  def testAvro2tfParsingFailure(): Unit = {

    val tensorizeInConfig = new File(
      getClass.getClassLoader.getResource(TENSORIZEIN_CONFIG_PATH_VALUE_2).getFile
    ).getAbsolutePath

    val params = Array(
      INPUT_PATHS_NAME, INPUT_PATHS_VALUE,
      WORKING_DIRECTORY_NAME, WORKING_DIRECTORY_VALUE,
      TENSORIZEIN_CONFIG_PATH_NAME, tensorizeInConfig,
      EXECUTION_MODE, "incorrect"
    )
    TensorizeInJobParamsParser.parse(params)
  }

  /**
   * Get TensorizeIn command line arguments
   *
   * @return A sequence of String
   */
  private def getCommandLineArguments(tensorizeInConfigPath: String): Seq[String] = {

    Seq(
      INPUT_PATHS_NAME, INPUT_PATHS_VALUE,
      WORKING_DIRECTORY_NAME, WORKING_DIRECTORY_VALUE,
      INPUT_DATE_RANGE_NAME, INPUT_DATE_RANGE_VALUE,
      NUM_OUTPUT_FILES_NAME, NUM_OUTPUT_FILES_VALUE,
      MIN_PARTS_NAME, MIN_PARTS_VALUE,
      ENABLE_SHUFFLE_NAME, ENABLE_SHUFFLE_VALUE,
      EXTERNAL_FEATURE_LIST_PATH_NAME, EXTERNAL_FEATURE_LIST_PATH_VALUE,
      TENSORIZEIN_CONFIG_PATH_NAME, tensorizeInConfigPath,
      ENABLE_TRAIN_MODE_NAME, ENABLE_TRAIN_MODE_VALUE
    )
  }

  /**
   * Get expected TensorizeIn parameters
   *
   * @param expectedTensorizeInConfig Expected TensorizeIn configuration
   * @return TensorizeIn parameters
   */
  private def getExpectedTensorizeInParams(expectedTensorizeInConfig: TensorizeInConfiguration): TensorizeInParams = {

    TensorizeInParams(
      inputPaths = mutable.ArraySeq(INPUT_PATHS_VALUE),
      workingDir = WorkingDirectory(WORKING_DIRECTORY_VALUE),
      inputDateRange = mutable.ArraySeq(INPUT_DATE_RANGE_VALUE_START, INPUT_DATE_RANGE_VALUE_END),
      inputDaysRange = mutable.ArraySeq.empty,
      numOfOutputFiles = Integer.parseInt(NUM_OUTPUT_FILES_VALUE),
      minParts = Integer.parseInt(MIN_PARTS_VALUE),
      enableShuffle = ENABLE_SHUFFLE_VALUE.toBoolean,
      externalFeaturesListPath = EXTERNAL_FEATURE_LIST_PATH_VALUE,
      tensorizeInConfig = expectedTensorizeInConfig,
      isTrainMode = ENABLE_TRAIN_MODE_VALUE.toBoolean,
      executionMode = Constants.TEST_EXECUTION_MODE,
      enableCache = ENABLE_CACHE_VALUE.toBoolean,
      skipConversion = SKIP_CONVERSION_VALUE.toBoolean,
      outputFormat = AVRO_RECORD.toString,
      extraColumnsToKeep = Seq.empty
    )
  }

  /**
   * Get expected TensorizeIn configuration
   *
   * @return TensorizeIn configuration
   */
  private def getExpectedTensorizeInConfig: TensorizeInConfiguration = {

    TensorizeInConfiguration(Seq(getExpectedFeature), Some(Seq(getExpectedLabel)))
  }

  /**
   * Get expected feature
   *
   * @return A feature
   */
  private def getExpectedFeature: Feature = {

    Feature(
      inputFeatureInfo = Some(InputFeatureInfo(
        Some(TENSORIZEIN_CONFIG_TEST_VALUE),
        None,
        Some(Map(TENSORIZEIN_CONFIG_TEST_INFO -> Map(TENSORIZEIN_CONFIG_TEST_INFO -> TENSORIZEIN_CONFIG_TEST_VALUE)))
      )),
      outputTensorInfo = OutputTensorInfo(
        TENSORIZEIN_CONFIG_TEST_VALUE,
        TENSORIZEIN_CONFIG_TEST_LONG_VALUE,
        Some(TENSORIZEIN_CONFIG_TEST_ARRAY)
      )
    )
  }

  /**
   * Get expected label
   *
   * @return A label in the Feature format
   */
  private def getExpectedLabel: Feature = {

    Feature(
      inputFeatureInfo = Some(InputFeatureInfo(
        Some(TENSORIZEIN_CONFIG_TEST_VALUE),
        None,
        None
      )),
      outputTensorInfo = OutputTensorInfo(
        TENSORIZEIN_CONFIG_TEST_VALUE,
        TENSORIZEIN_CONFIG_TEST_LONG_VALUE,
        None
      )
    )
  }

  /**
   * Test assert equals on TensorizeIn parameters without testing TensorizeIn configuration
   *
   * @param actualParams Actual TensorizeIn parameters
   * @param expectedParams Expected TensorizeIn parameters
   */
  private def testParamsEqualsWithoutTensorizeInConfig(actualParams: TensorizeInParams, expectedParams: TensorizeInParams): Unit = {

    assertEquals(actualParams.enableCache, expectedParams.enableCache)
    assertEquals(actualParams.enableShuffle, expectedParams.enableShuffle)
    assertEquals(actualParams.externalFeaturesListPath, expectedParams.externalFeaturesListPath)
    assertEquals(actualParams.inputDateRange, expectedParams.inputDateRange)
    assertEquals(actualParams.inputDaysRange, expectedParams.inputDaysRange)
    assertEquals(actualParams.inputPaths, expectedParams.inputPaths)
    assertEquals(actualParams.isTrainMode, expectedParams.isTrainMode)
    assertEquals(actualParams.executionMode, expectedParams.executionMode)
    assertEquals(actualParams.minParts, expectedParams.minParts)
    assertEquals(actualParams.numOfOutputFiles, expectedParams.numOfOutputFiles)
    assertEquals(actualParams.skipConversion, expectedParams.skipConversion)
    assertEquals(actualParams.workingDir, expectedParams.workingDir)
  }

  /**
   * Test assert equals on a sample of TensorizeIn configuration
   *
   * @param actualParams Actual TensorizeIn parameters
   * @param expectedParams Expected TensorizeIn parameters
   */
  private def testTensorizeInConfigEquals(actualParams: TensorizeInParams, expectedParams: TensorizeInParams): Unit = {

    val actualConfig = actualParams.tensorizeInConfig
    val actualFirstFeature = actualConfig.features.head
    val actualOutputTensorInfo = actualFirstFeature.outputTensorInfo

    val expectedConfig = expectedParams.tensorizeInConfig
    val expectedFirstFeature = expectedConfig.features.head
    val expectedOutputTensorInfo = expectedFirstFeature.outputTensorInfo

    assertEquals(actualFirstFeature.inputFeatureInfo, expectedFirstFeature.inputFeatureInfo)
    assertEquals(actualOutputTensorInfo.dtype, expectedOutputTensorInfo.dtype)
    assertEquals(actualOutputTensorInfo.name, expectedOutputTensorInfo.name)
    assertEquals(actualOutputTensorInfo.shape.isEmpty, expectedOutputTensorInfo.shape.isEmpty)
  }
}