package com.linkedin.avro2tf.jobs

import java.io.{File, FileOutputStream, PrintWriter}

import com.linkedin.avro2tf.parsers.TensorizeInJobParamsParser
import com.linkedin.avro2tf.utils.ConstantsForTest._
import com.linkedin.avro2tf.utils.{Constants, WithLocalSparkSession}
import org.apache.commons.io.FileUtils
import org.testng.Assert._
import org.testng.annotations.{DataProvider, Test}

class TensorizeInTest extends WithLocalSparkSession {

  /**
   * Data provider for TensorizeIn test
   *
   */
  @DataProvider
  def testData():Array[Array[Any]] = {
    Array(
      Array(TENSORIZEIN_CONFIG_PATH_VALUE_SAMPLE, INPUT_TEXT_FILE_PATHS, EXTERNAL_FEATURE_LIST_FILE_NAME_TEXT,
        WORKING_DIRECTORY_AVRO2TF),
      Array(TENSORIZEIN_CONFIG_PATH_VALUE_MOVIELENS, INPUT_MOVIELENS_FILE_PATHS, EXTERNAL_FEATURE_LIST_FILE_NAME_MOVIELENS,
        WORKING_DIRECTORY_AVRO2TF_MOVIELENS)
    )
  }

  /**
   * Test the correctness of TensorizeIn job
   *
   */
  @Test(dataProvider = "testData")
  def testAvro2tf(
    tensorizeInConfigPath: String,
    inputPath: String,
    externalFeatureListFileName: String,
    workingDirectory: String
  ): Unit = {

    val tensorizeInConfig = new File(
      getClass.getClassLoader.getResource(tensorizeInConfigPath).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(workingDirectory))

    // Set up external feature list
    val externalFeatureListFullPath = s"$workingDirectory/$EXTERNAL_FEATURE_LIST_PATH_TEXT"
    new File(externalFeatureListFullPath).mkdirs()
    new PrintWriter(new FileOutputStream(s"$externalFeatureListFullPath/$externalFeatureListFileName", false)) {
      write(SAMPLE_EXTERNAL_FEATURE_LIST)
      close()
    }

    val trainingParams = Array(
      INPUT_PATHS_NAME, inputPath,
      WORKING_DIRECTORY_NAME, workingDirectory,
      TENSORIZEIN_CONFIG_PATH_NAME, tensorizeInConfig,
      EXTERNAL_FEATURE_LIST_PATH_NAME, externalFeatureListFullPath
    )
    val tensorizeInTrainingParams = TensorizeInJobParamsParser.parse(trainingParams)

    TensorizeIn.run(session, tensorizeInTrainingParams)
    assertTrue(new File(s"${tensorizeInTrainingParams.workingDir.trainingDataPath}/_SUCCESS").exists())
    assertTrue(new File(tensorizeInTrainingParams.workingDir.tensorMetadataPath).exists())
    assertTrue(new File(tensorizeInTrainingParams.workingDir.featureListPath).exists())

    val validationParams = Array(
      INPUT_PATHS_NAME, inputPath,
      WORKING_DIRECTORY_NAME, workingDirectory,
      TENSORIZEIN_CONFIG_PATH_NAME, tensorizeInConfig,
      EXECUTION_MODE, Constants.VALIDATION_EXECUTION_MODE
    )
    val tensorizeInValidationParams = TensorizeInJobParamsParser.parse(validationParams)

    TensorizeIn.run(session, tensorizeInValidationParams)
    assertTrue(new File(s"${tensorizeInValidationParams.workingDir.validationDataPath}/_SUCCESS").exists())

    val testParams = Array(
      INPUT_PATHS_NAME, inputPath,
      WORKING_DIRECTORY_NAME, workingDirectory,
      TENSORIZEIN_CONFIG_PATH_NAME, tensorizeInConfig,
      EXECUTION_MODE, Constants.TEST_EXECUTION_MODE
    )
    val tensorizeInTestParams = TensorizeInJobParamsParser.parse(testParams)

    TensorizeIn.run(session, tensorizeInTestParams)
    assertTrue(new File(s"${tensorizeInTestParams.workingDir.testDataPath}/_SUCCESS").exists())
  }
}