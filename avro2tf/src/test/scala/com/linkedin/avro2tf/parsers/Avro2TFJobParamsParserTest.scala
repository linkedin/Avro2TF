package com.linkedin.avro2tf.parsers

import java.io.File

import scala.collection.mutable

import com.linkedin.avro2tf.configs._
import com.linkedin.avro2tf.constants.Avro2TFJobParamNames
import com.linkedin.avro2tf.utils.{TestUtil, TrainingMode}
import com.linkedin.avro2tf.utils.ConstantsForTest._
import org.testng.Assert._
import org.testng.annotations.{DataProvider, Test}

/**
 * Test the parser file for Avro2TF job parameters from command line arguments
 *
 */
class Avro2TFJobParamsParserTest {

  /**
   * Test if the command line argument parser can work properly when well-formed parameters are specified
   *
   */
  @Test
  def testParse(): Unit = {

    // Get expected Avro2TF configuration and parameters
    val expectedAvro2TFConfig = getExpectedAvro2TFConfig

    val expectedAvro2TFParams = getExpectedAvro2TFParams(expectedAvro2TFConfig)

    // Get actual Avro2TF configuration and parameters
    val avro2TFConfigPath = new File(
      getClass.getClassLoader.getResource(AVRO2TF_CONFIG_PATH_VALUE_2).getFile
    ).getAbsolutePath
    val arguments = getCommandLineArguments(avro2TFConfigPath)
    val actualAvro2TFParams = Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(arguments))

    // Test assert equals on Avro2TF parameters without testing Avro2T configuration
    testParamsEqualsWithoutAvro2TFConfig(actualAvro2TFParams, expectedAvro2TFParams)

    // Test assert equals on a sample of Avro2T configuration
    testAvro2TFConfigEquals(actualAvro2TFParams, expectedAvro2TFParams)
  }

  /**
   * Test if the command line argument parser can work properly when a default column expression need to be added
   *
   */
  @Test
  def testParseWithDefaultColExpr(): Unit = {

    // Get expected Avro2TF configuration and parameters
    val expectedAvro2TFConfig = getExpectedAvro2TFConfig
    val expectedAvro2TFParams = getExpectedAvro2TFParams(expectedAvro2TFConfig)

    // Get actual Avro2TF configuration and parameters
    val avro2TFConfigPath = new File(
      getClass.getClassLoader.getResource(AVRO2TF_CONFIG_PATH_VALUE_3).getFile
    ).getAbsolutePath
    val arguments = getCommandLineArguments(avro2TFConfigPath)
    val actualAvro2TFParams = Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(arguments))

    // Test assert equals on Avro2TF parameters without testing Avro2TF configuration
    testParamsEqualsWithoutAvro2TFConfig(actualAvro2TFParams, expectedAvro2TFParams)

    // Test assert equals on a sample of Avro2TF configuration
    testAvro2TFConfigEquals(actualAvro2TFParams, expectedAvro2TFParams)
  }

  /**
   * Test if the command line argument parser can throw exception when both column expression and column configuration exist
   *
   */
  @Test(expectedExceptions = Array(classOf[IllegalArgumentException]))
  def testParseWithBothColExprAndColConfig(): Unit = {

    // Get actual Avro2TF configuration and parameters
    val avro2TFConfigPath = new File(
      getClass.getClassLoader.getResource(AVRO2TF_CONFIG_PATH_VALUE_4).getFile
    ).getAbsolutePath
    val arguments = getCommandLineArguments(avro2TFConfigPath)

    Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(arguments))
  }

  /**
   * Test if the command line argument parser can work properly when a default shape need to be added
   *
   */
  @Test
  def testParseWithDefaultShape(): Unit = {

    // Get expected Avro2TF configuration and parameters
    val expectedAvro2TFConfig = getExpectedAvro2TFConfig
    val expectedAvro2TFParams = getExpectedAvro2TFParams(expectedAvro2TFConfig)

    // Get actual Avro2TF configuration and parameters
    val avro2TFConfigPath = new File(
      getClass.getClassLoader.getResource(AVRO2TF_CONFIG_PATH_VALUE_5).getFile
    ).getAbsolutePath
    val arguments = getCommandLineArguments(avro2TFConfigPath)
    val actualAvro2TFParams = Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(arguments))

    // Test assert equals on Avro2TF parameters without testing Avro2TF configuration
    testParamsEqualsWithoutAvro2TFConfig(actualAvro2TFParams, expectedAvro2TFParams)

    // Test assert equals on a sample of Avro2TF configuration
    testAvro2TFConfigEquals(actualAvro2TFParams, expectedAvro2TFParams)
  }

  /**
   * Test if the command line argument parser can throw exception when both input date and days range exist
   *
   */
  @Test(expectedExceptions = Array(classOf[IllegalArgumentException]))
  def testParseWithBothDateAndDaysRange(): Unit = {

    // Get actual Avro2TF configuration and parameters
    val avro2TFConfigPath = new File(
      getClass.getClassLoader.getResource(AVRO2TF_CONFIG_PATH_VALUE_2).getFile
    ).getAbsolutePath
    val arguments = getCommandLineArguments(avro2TFConfigPath) ++ Map(Avro2TFJobParamNames.INPUT_DAYS_RANGE -> INPUT_DAYS_RANGE_VALUE)

    Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(arguments))
  }

  /**
   * Test that the parsing fails when incorrect execution mode is passed
   *
   */
  @Test(expectedExceptions = Array(classOf[IllegalArgumentException]))
  def testAvro2tfParsingFailure(): Unit = {

    val avro2TFConfig = new File(
      getClass.getClassLoader.getResource(AVRO2TF_CONFIG_PATH_VALUE_2).getFile
    ).getAbsolutePath

    val params = Map(
      Avro2TFJobParamNames.INPUT_PATHS -> INPUT_PATHS_VALUE,
      Avro2TFJobParamNames.WORKING_DIR -> WORKING_DIRECTORY_VALUE,
      Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> avro2TFConfig,
      Avro2TFJobParamNames.EXECUTION_MODE -> "incorrect"
    )
    Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(params))
  }

  /**
   * Data provider for feature indices conversion test
   *
   */
  @DataProvider()
  def invalidFeatureSharingSetting(): Array[Array[String]] = {

    Array(
      Array("a; b,c"),
      Array("a,b;b,c,d"),
      Array(""),
      Array(";")
    )
  }

  /**
   * Test that parsing fails when incorrect feature list sharing settings are passed.
   * Case a) some feature list group contains only one output tensor
   * Case b) the same output tensor appears in more than 1 feature list sharing group
   *
   */
  @Test(expectedExceptions = Array(classOf[IllegalArgumentException]), dataProvider = "invalidFeatureSharingSetting")
  def testInvalidFeatureSharingSettings(tensors_sharing_feature_lists: String): Unit = {

    val avro2TFConfig = new File(
      getClass.getClassLoader.getResource(AVRO2TF_CONFIG_PATH_VALUE_2).getFile
    ).getAbsolutePath

    val params = Map(
      Avro2TFJobParamNames.INPUT_PATHS -> INPUT_PATHS_VALUE,
      Avro2TFJobParamNames.WORKING_DIR -> WORKING_DIRECTORY_VALUE,
      Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> avro2TFConfig,
      Avro2TFJobParamNames.TENSORS_SHARING_FEATURE_LISTS -> tensors_sharing_feature_lists
    )
    Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(params))
  }

  /**
   * Data provider for feature list sharing configurations
   *
   */
  @DataProvider()
  def validFeatureSharingSetting(): Array[Array[Any]] = {

    Array(
      Array("a,b", Array(Array("a", "b"))),
      Array("a,b;", Array(Array("a", "b"))),
      Array("  a, b ;", Array(Array("a", "b"))),
      Array(" a, b ; c,d,e;", Array(Array("a", "b"), Array("c", "d", "e"))),
      Array(" a, b ; c,d,e ", Array(Array("a", "b"), Array("c", "d", "e")))
    )
  }

  /**
   * Test if the command line argument parser can work properly when shared feature lists settings are added
   *
   */
  @Test(dataProvider = "validFeatureSharingSetting")
  def testParseSharingFeatureLists(config_input: String, parsed_param: Array[Array[String]]): Unit = {

    // Get expected Avro2TF configuration and parameters
    val expectedAvro2TFConfig = getExpectedAvro2TFConfig
    var expectedAvro2TFParams = getExpectedAvro2TFParams(expectedAvro2TFConfig)
    // Update the expected Avro2TF params to include feature list sharing configurations
    expectedAvro2TFParams = expectedAvro2TFParams
      .copy(tensorsSharingFeatureLists = parsed_param)

    // Get actual Avro2TF configuration and parameters
    val avro2TFConfigPath = new File(
      getClass.getClassLoader.getResource(AVRO2TF_CONFIG_PATH_VALUE_5).getFile
    ).getAbsolutePath
    var arguments = getCommandLineArguments(avro2TFConfigPath)
    arguments = arguments ++ Map(Avro2TFJobParamNames.TENSORS_SHARING_FEATURE_LISTS -> config_input)
    val actualAvro2TFParams = Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(arguments))

    // Test assert equals on Avro2TF parameters without testing Avro2TF configuration
    testParamsEqualsWithoutAvro2TFConfig(actualAvro2TFParams, expectedAvro2TFParams)

    // Test assert equals on a sample of Avro2TF configuration
    testAvro2TFConfigEquals(actualAvro2TFParams, expectedAvro2TFParams)
  }

  /**
   * Get Avro2TF command line arguments
   *
   * @return A sequence of String
   */
  private def getCommandLineArguments(avro2TFConfigPath: String): Map[String, Any] = {

    Map(
      Avro2TFJobParamNames.INPUT_PATHS -> INPUT_PATHS_VALUE,
      Avro2TFJobParamNames.WORKING_DIR -> WORKING_DIRECTORY_VALUE,
      Avro2TFJobParamNames.INPUT_DATE_RANGE -> INPUT_DATE_RANGE_VALUE,
      Avro2TFJobParamNames.NUM_OUTPUT_FILES -> NUM_OUTPUT_FILES_VALUE,
      Avro2TFJobParamNames.MIN_PARTS -> MIN_PARTS_VALUE,
      Avro2TFJobParamNames.SHUFFLE -> ENABLE_SHUFFLE_VALUE,
      Avro2TFJobParamNames.EXECUTION_MODE -> TrainingMode.test,
      Avro2TFJobParamNames.EXTERNAL_FEATURE_LIST_PATH -> EXTERNAL_FEATURE_LIST_PATH_VALUE,
      Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> avro2TFConfigPath
    )
  }

  /**
   * Get expected Avro2TF parameters
   *
   * @param expectedAvro2TFConfig Expected Avro2TF configuration
   * @return Avro2TF parameters
   */
  private def getExpectedAvro2TFParams(expectedAvro2TFConfig: Avro2TFConfiguration): Avro2TFParams = {

    Avro2TFParams(
      inputPaths = mutable.ArraySeq(INPUT_PATHS_VALUE),
      workingDir = WorkingDirectory(WORKING_DIRECTORY_VALUE),
      inputDateRange = mutable.ArraySeq(INPUT_DATE_RANGE_VALUE_START, INPUT_DATE_RANGE_VALUE_END),
      inputDaysRange = mutable.ArraySeq.empty,
      numOfOutputFiles = NUM_OUTPUT_FILES_VALUE,
      minParts = MIN_PARTS_VALUE,
      enableShuffle = ENABLE_SHUFFLE_VALUE,
      externalFeaturesListPath = EXTERNAL_FEATURE_LIST_PATH_VALUE,
      avro2TFConfig = expectedAvro2TFConfig,
      executionMode = TrainingMode.test,
      enableCache = ENABLE_CACHE_VALUE,
      skipConversion = SKIP_CONVERSION_VALUE.toBoolean,
      outputFormat = AVRO_RECORD.toString,
      extraColumnsToKeep = Seq.empty,
      tensorsSharingFeatureLists = Array[Array[String]](),
      numPartitions = 100,
      partitionFieldName = "",
      termOnlyFeatureList = false,
      discardUnknownEntries = false,
      enableFilterZero = false,
      passThroughOnly = false
    )
  }

  /**
   * Get expected Avro2TF configuration
   *
   * @return Avro2TF configuration
   */
  private def getExpectedAvro2TFConfig: Avro2TFConfiguration = {

    Avro2TFConfiguration(Seq(getExpectedFeature), Seq(getExpectedLabel))
  }

  /**
   * Get expected feature
   *
   * @return A feature
   */
  private def getExpectedFeature: Feature = {

    Feature(
      inputFeatureInfo = Some(
        InputFeatureInfo(
          Some(AVRO2TF_CONFIG_TEST_VALUE),
          None,
          Some(TransformConfig(Some(HashInfo(1)), Some(Tokenization(true))))
        )),
      outputTensorInfo = OutputTensorInfo(
        AVRO2TF_CONFIG_TEST_VALUE,
        AVRO2TF_CONFIG_TEST_LONG_VALUE,
        AVRO2TF_CONFIG_TEST_ARRAY
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
      inputFeatureInfo = Some(
        InputFeatureInfo(
          Some(AVRO2TF_CONFIG_TEST_VALUE),
          None,
          None
        )),
      outputTensorInfo = OutputTensorInfo(
        AVRO2TF_CONFIG_TEST_VALUE,
        AVRO2TF_CONFIG_TEST_LONG_VALUE,
        Seq()
      )
    )
  }

  /**
   * Test assert equals on Avro2TF parameters without testing Avro2TF configuration
   *
   * @param actualParams Actual Avro2TF parameters
   * @param expectedParams Expected Avro2TF parameters
   */
  private def testParamsEqualsWithoutAvro2TFConfig(
    actualParams: Avro2TFParams,
    expectedParams: Avro2TFParams): Unit = {

    assertEquals(actualParams.enableCache, expectedParams.enableCache)
    assertEquals(actualParams.enableShuffle, expectedParams.enableShuffle)
    assertEquals(actualParams.externalFeaturesListPath, expectedParams.externalFeaturesListPath)
    assertEquals(actualParams.inputDateRange, expectedParams.inputDateRange)
    assertEquals(actualParams.inputDaysRange, expectedParams.inputDaysRange)
    assertEquals(actualParams.inputPaths, expectedParams.inputPaths)
    assertEquals(actualParams.executionMode, expectedParams.executionMode)
    assertEquals(actualParams.minParts, expectedParams.minParts)
    assertEquals(actualParams.numOfOutputFiles, expectedParams.numOfOutputFiles)
    assertEquals(actualParams.skipConversion, expectedParams.skipConversion)
    assertEquals(actualParams.workingDir, expectedParams.workingDir)
  }

  /**
   * Test assert equals on a sample of Avro2TF configuration
   *
   * @param actualParams Actual Avro2TF parameters
   * @param expectedParams Expected Avro2TF parameters
   */
  private def testAvro2TFConfigEquals(actualParams: Avro2TFParams, expectedParams: Avro2TFParams): Unit = {

    val actualConfig = actualParams.avro2TFConfig
    val actualFirstFeature = actualConfig.features.head
    val actualOutputTensorInfo = actualFirstFeature.outputTensorInfo

    val expectedConfig = expectedParams.avro2TFConfig
    val expectedFirstFeature = expectedConfig.features.head
    val expectedOutputTensorInfo = expectedFirstFeature.outputTensorInfo

    assertEquals(actualFirstFeature.inputFeatureInfo, expectedFirstFeature.inputFeatureInfo)
    assertEquals(actualOutputTensorInfo.dtype, expectedOutputTensorInfo.dtype)
    assertEquals(actualOutputTensorInfo.name, expectedOutputTensorInfo.name)
    assertEquals(actualOutputTensorInfo.shape.isEmpty, expectedOutputTensorInfo.shape.isEmpty)
  }
}