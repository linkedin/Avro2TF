package com.linkedin.avro2tf.jobs

import java.io.File

import com.databricks.spark.avro._
import com.linkedin.avro2tf.helpers.TensorizeInJobHelper
import com.linkedin.avro2tf.parsers.TensorizeInJobParamsParser
import com.linkedin.avro2tf.utils.ConstantsForTest._
import com.linkedin.avro2tf.utils.{CommonUtils, TestUtil, WithLocalSparkSession}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types._
import org.testng.Assert._
import org.testng.annotations.{DataProvider, Test}

class FeatureIndicesConversionTest extends WithLocalSparkSession {

  /**
   * Data provider for feature indices conversion test
   *
   */
  @DataProvider
  def testData():Array[Array[Any]] = {
    Array(
      Array(AVRO_RECORD),
      Array(TF_RECORD)
    )
  }

  /**
   * Test the correctness of indices conversion job
   */
  @Test(dataProvider = "testData")
  def testConversion(outputFormat: String): Unit = {

    val tensorizeInConfig = new File(
      getClass.getClassLoader.getResource(TENSORIZEIN_CONFIG_PATH_VALUE_SAMPLE).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(WORKING_DIRECTORY_INDICES_CONVERSION))

    val params = Array(
      INPUT_PATHS_NAME, INPUT_TEXT_FILE_PATHS,
      WORKING_DIRECTORY_NAME, WORKING_DIRECTORY_INDICES_CONVERSION,
      TENSORIZEIN_CONFIG_PATH_NAME, tensorizeInConfig,
      OUTPUT_FORMAT_NAME, outputFormat
    )
    val dataFrame = session.read.avro(INPUT_TEXT_FILE_PATHS)
    val tensorizeInParams = TensorizeInJobParamsParser.parse(params)

    val dataFrameExtracted = (new FeatureExtraction).run(dataFrame, tensorizeInParams)
    val dataFrameTransformed = (new FeatureTransformation).run(dataFrameExtracted, tensorizeInParams)
    (new FeatureListGeneration).run(dataFrameTransformed, tensorizeInParams)
    val convertedDataFrame = (new FeatureIndicesConversion).run(dataFrameTransformed, tensorizeInParams)

    TestUtil.checkOutputColumns(convertedDataFrame, tensorizeInParams)

    // check if the type of "wordSeq" column is the expected Seq[Long]
    val convertedTextColummType = convertedDataFrame.schema(FEATURE_WORD_SEQ_COL_NAME).dataType
    assertTrue(CommonUtils.isArrayOfLong(convertedTextColummType))

    // check if the type of "words_wideFeatures" column is the expected SparseVector type
    val convertedNTVColummType = convertedDataFrame.schema(FEATURE_WORDS_WIDE_FEATURES_COL_NAME).dataType
    assertTrue(convertedNTVColummType.isInstanceOf[StructType])
    val convertedNTVStructType = convertedNTVColummType.asInstanceOf[StructType]
    assertEquals(
      convertedNTVStructType.fieldNames.toSet,
      Set(SPARSE_VECTOR_INDICES_FIELD_NAME, SPARSE_VECTOR_VALUES_FIELD_NAME))
    assertTrue(CommonUtils.isArrayOfLong(convertedNTVStructType(SPARSE_VECTOR_INDICES_FIELD_NAME).dataType))
    assertTrue(CommonUtils.isArrayOfFloat(convertedNTVStructType(SPARSE_VECTOR_VALUES_FIELD_NAME).dataType))

    TensorizeInJobHelper.saveDataToHDFS(convertedDataFrame, tensorizeInParams)
    assertTrue(new File(s"${tensorizeInParams.workingDir.trainingDataPath}/_SUCCESS").exists())
  }
}