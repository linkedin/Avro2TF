package com.linkedin.avro2tf.jobs

import java.io.File

import com.databricks.spark.avro._
import com.linkedin.avro2tf.helpers.TensorizeInJobHelper
import com.linkedin.avro2tf.parsers.TensorizeInJobParamsParser
import com.linkedin.avro2tf.utils.ConstantsForTest._
import com.linkedin.avro2tf.utils.{TestUtil, WithLocalSparkSession}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.StringType
import org.testng.Assert._
import org.testng.annotations.Test

class FeatureExtractionTest extends WithLocalSparkSession {

  /**
   * Test if the Feature Extraction job can finish successfully
   *
   */
  @Test
  def testFeatureExtraction(): Unit = {

    val tensorizeInConfig = new File(
      getClass.getClassLoader.getResource(TENSORIZEIN_CONFIG_PATH_VALUE_SAMPLE).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(WORKING_DIRECTORY_FEATURE_EXTRACTION_TEXT))

    val params = Array(
      INPUT_PATHS_NAME, INPUT_TEXT_FILE_PATHS,
      WORKING_DIRECTORY_NAME, WORKING_DIRECTORY_FEATURE_EXTRACTION_TEXT,
      TENSORIZEIN_CONFIG_PATH_NAME, tensorizeInConfig
    )

    val dataFrame = session.read.avro(INPUT_TEXT_FILE_PATHS)
    val tensorizeInParams = TensorizeInJobParamsParser.parse(params)

    val result = (new FeatureExtraction).run(dataFrame, tensorizeInParams)

    TensorizeInJobHelper.saveDataToHDFS(result, tensorizeInParams)

    // Check if the actual columns of the result DataFrame match those specified in TensorizeIn parameters
    TestUtil.checkOutputColumns(result, tensorizeInParams)
    // Check a sample feature is successfully extracted to an output column
    assertTrue(result.schema(FEATURE_WORD_SEQ_COL_NAME).dataType.isInstanceOf[StringType])
    assertTrue(new File(s"${tensorizeInParams.workingDir.trainingDataPath}/_SUCCESS").exists())
  }
}