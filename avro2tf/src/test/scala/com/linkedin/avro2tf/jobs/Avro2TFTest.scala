package com.linkedin.avro2tf.jobs

import java.io.{File, FileOutputStream, PrintWriter}

import com.linkedin.avro2tf.constants.Avro2TFJobParamNames
import com.linkedin.avro2tf.parsers.Avro2TFJobParamsParser
import com.linkedin.avro2tf.utils.ConstantsForTest._
import com.linkedin.avro2tf.utils.{CommonUtils, IOUtils, TestUtil, TrainingMode, WithLocalSparkSession}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.testng.Assert._
import org.testng.annotations.{DataProvider, Test}

class Avro2TFTest extends WithLocalSparkSession {

  /**
   * Data provider for Avro2TF test
   *
   */
  @DataProvider
  def testData(): Array[Array[Any]] = {

    Array(
      Array(
        AVRO2TF_CONFIG_PATH_VALUE_SAMPLE, INPUT_TEXT_FILE_PATHS, EXTERNAL_FEATURE_LIST_FILE_NAME_TEXT,
        WORKING_DIRECTORY_AVRO2TF),
      Array(
        AVRO2TF_CONFIG_PATH_VALUE_MOVIELENS, INPUT_MOVIELENS_FILE_PATHS, EXTERNAL_FEATURE_LIST_FILE_NAME_MOVIELENS,
        WORKING_DIRECTORY_AVRO2TF_MOVIELENS)
    )
  }

  /**
   * Test the correctness of Avro2TF job
   *
   */
  @Test(dataProvider = "testData")
  def testAvro2tf(
    avro2TFConfigPath: String,
    inputPath: String,
    externalFeatureListFileName: String,
    workingDirectory: String
  ): Unit = {

    val avro2TFConfig = new File(
      getClass.getClassLoader.getResource(avro2TFConfigPath).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(workingDirectory))

    // Set up external feature list
    val externalFeatureListFullPath = s"$workingDirectory/$EXTERNAL_FEATURE_LIST_PATH_TEXT"
    new File(externalFeatureListFullPath).mkdirs()
    new PrintWriter(new FileOutputStream(s"$externalFeatureListFullPath/$externalFeatureListFileName", false)) {
      write(SAMPLE_EXTERNAL_FEATURE_LIST)
      close()
    }

    val trainingParams = Map(
      Avro2TFJobParamNames.INPUT_PATHS -> inputPath,
      Avro2TFJobParamNames.WORKING_DIR -> workingDirectory,
      Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> avro2TFConfig,
      Avro2TFJobParamNames.EXTERNAL_FEATURE_LIST_PATH -> externalFeatureListFullPath
    )
    val avro2TFTrainingParams = Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(trainingParams))

    Avro2TF.run(session, avro2TFTrainingParams)
    assertTrue(new File(s"${avro2TFTrainingParams.workingDir.trainingDataPath}/_SUCCESS").exists())
    assertTrue(new File(avro2TFTrainingParams.workingDir.tensorMetadataPath).exists())
    assertTrue(new File(avro2TFTrainingParams.workingDir.featureListPath).exists())

    val validationParams = Map(
      Avro2TFJobParamNames.INPUT_PATHS -> inputPath,
      Avro2TFJobParamNames.WORKING_DIR -> workingDirectory,
      Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> avro2TFConfig,
      Avro2TFJobParamNames.EXECUTION_MODE -> TrainingMode.validation.toString
    )
    val avro2TFValidationParams = Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(validationParams))

    Avro2TF.run(session, avro2TFValidationParams)
    assertTrue(new File(s"${avro2TFValidationParams.workingDir.validationDataPath}/_SUCCESS").exists())

    val testParams = Map(
      Avro2TFJobParamNames.INPUT_PATHS -> inputPath,
      Avro2TFJobParamNames.WORKING_DIR -> workingDirectory,
      Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> avro2TFConfig,
      Avro2TFJobParamNames.EXECUTION_MODE -> TrainingMode.test.toString
    )
    val avro2TFTestParams = Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(testParams))

    Avro2TF.run(session, avro2TFTestParams)
    assertTrue(new File(s"${avro2TFTestParams.workingDir.testDataPath}/_SUCCESS").exists())
  }

  /**
   * Data provider for testing invalid feature list sharing settings
   *
   */
  @DataProvider
  def testDataWithInvalidFeatureListSharing(): Array[Array[Any]] = {

    Array(
      Array(
        AVRO2TF_CONFIG_PATH_VALUE_SAMPLE, INPUT_TEXT_FILE_PATHS, WORKING_DIRECTORY_AVRO2TF,
        "firstWord,dummyName"),
      Array(
        AVRO2TF_CONFIG_PATH_VALUE_MOVIELENS, INPUT_MOVIELENS_FILE_PATHS, WORKING_DIRECTORY_AVRO2TF_MOVIELENS,
        "userId,dummyName"
      )
    )
  }

  /**
   * Test correctly throw exception if invalid tensor names exist in sharing feature list setting
   *
   */
  @Test(
    expectedExceptions = Array(classOf[IllegalArgumentException]),
    expectedExceptionsMessageRegExp = "Invalid output tensor name in --tensors-sharing-feature-lists.*",
    dataProvider = "testDataWithInvalidFeatureListSharing")
  def testFailOnInvalidFeatureListSharingSetting(
    avro2TFConfigPath: String,
    inputPath: String,
    workingDirectory: String,
    tensorsSharingFeatureLists: String
  ): Unit = {

    val avro2TFConfig = new File(
      getClass.getClassLoader.getResource(avro2TFConfigPath).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(workingDirectory))

    val trainingParams = Map(
      Avro2TFJobParamNames.INPUT_PATHS -> inputPath,
      Avro2TFJobParamNames.WORKING_DIR -> workingDirectory,
      Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> avro2TFConfig,
      Avro2TFJobParamNames.TENSORS_SHARING_FEATURE_LISTS -> tensorsSharingFeatureLists
    )
    val avro2TFTrainingParams = Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(trainingParams))
    Avro2TF.run(session, avro2TFTrainingParams)
  }

  /**
    * Test out by providing different config with "enable-filter-zero", fill in source data with zeros and output format
    * as sparse:
    * if zeros can be filtered from output if "enable-filter-zero" is set as true
    * if zeros can be kept from output if "enable-filter-zero" is set as false
    */
  @Test
  def testEnableFilterZero(): Unit = {
    val workingDirectory = "/tmp/avro2tf-test"
    val avro2TFConfig = new File(
      getClass.getClassLoader.getResource(AVRO2TF_CONFIG_PATH_VALUE_SAMPLE).getFile).getAbsolutePath
    FileUtils.deleteDirectory(new File(workingDirectory))

    // Set up external feature list
    val externalFeatureListFullPath = s"$workingDirectory/$EXTERNAL_FEATURE_LIST_PATH_TEXT"
    new File(externalFeatureListFullPath).mkdirs()
    new PrintWriter(new FileOutputStream(s"$externalFeatureListFullPath/$EXTERNAL_FEATURE_LIST_FILE_NAME_TEXT", false)) {
      write(SAMPLE_EXTERNAL_FEATURE_LIST)
      close()
    }

    val avro2TFTrainingParams = Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(
      Map(
        Avro2TFJobParamNames.INPUT_PATHS -> INPUT_TEXT_FILE_PATHS,
        Avro2TFJobParamNames.WORKING_DIR -> workingDirectory,
        Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> avro2TFConfig,
        Avro2TFJobParamNames.EXTERNAL_FEATURE_LIST_PATH -> externalFeatureListFullPath
      )
    ))

    val workingDirectoryFilterZero = "/tmp/avro2tf-test-filterZero"
    FileUtils.deleteDirectory(new File(workingDirectoryFilterZero))

    val avro2TFTrainingParamsEnableFilterZero = Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(
      Map(
        Avro2TFJobParamNames.INPUT_PATHS -> INPUT_TEXT_FILE_PATHS,
        Avro2TFJobParamNames.WORKING_DIR -> workingDirectoryFilterZero,
        Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> avro2TFConfig,
        Avro2TFJobParamNames.EXTERNAL_FEATURE_LIST_PATH -> externalFeatureListFullPath,
        Avro2TFJobParamNames.ENABLE_FILTER_ZERO -> "true"
      )
    ))


    Avro2TF.run(session, avro2TFTrainingParams)
    assertTrue(new File(s"${avro2TFTrainingParams.workingDir.trainingDataPath}/_SUCCESS").exists())
    assertTrue(new File(avro2TFTrainingParams.workingDir.tensorMetadataPath).exists())
    assertTrue(new File(avro2TFTrainingParams.workingDir.featureListPath).exists())

    val output = IOUtils.readAvro(session, avro2TFTrainingParams.workingDir.trainingDataPath)
    val last = output.collect().last

    Avro2TF.run(session, avro2TFTrainingParamsEnableFilterZero)
    assertTrue(new File(s"${avro2TFTrainingParamsEnableFilterZero.workingDir.trainingDataPath}/_SUCCESS").exists())
    assertTrue(new File(avro2TFTrainingParamsEnableFilterZero.workingDir.tensorMetadataPath).exists())
    assertTrue(new File(avro2TFTrainingParamsEnableFilterZero.workingDir.featureListPath).exists())

    val outputFilterZero = IOUtils.readAvro(session, avro2TFTrainingParamsEnableFilterZero.workingDir.trainingDataPath)
    val lastFilterZero = outputFilterZero.collect().last

    assertEquals(last.schema, lastFilterZero.schema)

    val sparseVectorNames = last.schema.filter(elem => CommonUtils.isSparseTensor(elem.dataType)).map(_.name)

    sparseVectorNames.foreach {
      name =>
        assertFalse(last.getAs[Row](name).getAs[Seq[Float]](1).filter(_ == 0.0).isEmpty)
        assertTrue(lastFilterZero.getAs[Row](name).getAs[Seq[Float]](1).filter(_ == 0.0).isEmpty)
    }
  }
}