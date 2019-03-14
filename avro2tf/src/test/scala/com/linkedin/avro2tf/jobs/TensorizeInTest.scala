package com.linkedin.avro2tf.jobs

import java.io.{File, FileOutputStream, PrintWriter}

import com.linkedin.avro2tf.parsers.TensorizeInJobParamsParser
import com.linkedin.avro2tf.utils.ConstantsForTest._
import com.linkedin.avro2tf.utils.WithLocalSparkSession

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

    val params = Array(
      INPUT_PATHS_NAME, inputPath,
      WORKING_DIRECTORY_NAME, workingDirectory,
      TENSORIZEIN_CONFIG_PATH_NAME, tensorizeInConfig,
      EXTERNAL_FEATURE_LIST_PATH_NAME, externalFeatureListFullPath
    )
    val tensorizeInParams = TensorizeInJobParamsParser.parse(params)

    TensorizeIn.run(session, tensorizeInParams)

    assertTrue(new File(s"${tensorizeInParams.workingDir.trainingDataPath}/_SUCCESS").exists())
    assertTrue(new File(tensorizeInParams.workingDir.tensorMetadataPath).exists())
    assertTrue(new File(tensorizeInParams.workingDir.featureListPath).exists())
  }
}