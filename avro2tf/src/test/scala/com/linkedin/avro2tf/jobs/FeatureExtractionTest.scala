package com.linkedin.avro2tf.jobs

import java.io.File

import com.databricks.spark.avro._
import com.linkedin.avro2tf.constants.Avro2TFJobParamNames
import com.linkedin.avro2tf.helpers.Avro2TFJobHelper
import com.linkedin.avro2tf.parsers.Avro2TFJobParamsParser
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

    val avro2TFConfig = new File(
      getClass.getClassLoader.getResource(AVRO2TF_CONFIG_PATH_VALUE_SAMPLE).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(WORKING_DIRECTORY_FEATURE_EXTRACTION_TEXT))

    val params = Map(
      Avro2TFJobParamNames.INPUT_PATHS -> INPUT_TEXT_FILE_PATHS,
      Avro2TFJobParamNames.WORKING_DIR -> WORKING_DIRECTORY_FEATURE_EXTRACTION_TEXT,
      Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> avro2TFConfig,
      Avro2TFJobParamNames.EXTRA_COLUMNS_TO_KEEP -> EXTRA_COLUMNS_TO_KEEP_VALUE
    )

    val dataFrame = session.read.avro(INPUT_TEXT_FILE_PATHS)
    val avro2TFParams = Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(params))

    val result = FeatureExtraction.run(dataFrame, avro2TFParams)

    Avro2TFJobHelper.saveDataToHDFS(result, avro2TFParams)

    // Check if the actual columns of the result DataFrame match those specified in Avro2TF parameters
    TestUtil.checkOutputColumns(result, avro2TFParams)
    // Check a sample feature is successfully extracted to an output column
    assertTrue(result.schema(FEATURE_WORD_SEQ_COL_NAME).dataType.isInstanceOf[StringType])
    assertTrue(new File(s"${avro2TFParams.workingDir.trainingDataPath}/_SUCCESS").exists())
  }
}