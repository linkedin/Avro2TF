package com.linkedin.avro2tf.jobs

import java.io.File

import com.databricks.spark.avro._
import com.linkedin.avro2tf.helpers.TensorizeInJobHelper
import com.linkedin.avro2tf.parsers.TensorizeInJobParamsParser
import com.linkedin.avro2tf.utils.ConstantsForTest._
import com.linkedin.avro2tf.utils.{TestUtil, WithLocalSparkSession}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.ArrayType
import org.testng.Assert._
import org.testng.annotations.Test

class FeatureTransformationTest extends WithLocalSparkSession {

  /**
   * Test if the Feature Transformation job for text features can finish successfully
   *
   */
  @Test
  def testTextFeatureTransformation(): Unit = {

    val tensorizeInConfig = new File(
      getClass.getClassLoader.getResource(TENSORIZEIN_CONFIG_PATH_VALUE_SAMPLE).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(WORKING_DIRECTORY_FEATURE_TRANSFORMATION_TEXT))

    val params = Array(
      INPUT_PATHS_NAME, INPUT_TEXT_FILE_PATHS,
      WORKING_DIRECTORY_NAME, WORKING_DIRECTORY_FEATURE_TRANSFORMATION_TEXT,
      TENSORIZEIN_CONFIG_PATH_NAME, tensorizeInConfig
    )

    val dataFrame = session.read.avro(INPUT_TEXT_FILE_PATHS)
    val tensorizeInParams = TensorizeInJobParamsParser.parse(params)

    val dataFrameExtracted = FeatureExtraction.run(dataFrame, tensorizeInParams)
    val result = FeatureTransformation.run(dataFrameExtracted, tensorizeInParams)

    TensorizeInJobHelper.saveDataToHDFS(result, tensorizeInParams)

    // Check if the actual columns of the result DataFrame match those specified in TensorizeIn parameters
    TestUtil.checkOutputColumns(result, tensorizeInParams)
    // Check text feature is successfully transformed to Strings
    assertTrue(result.schema(FEATURE_WORD_SEQ_COL_NAME).dataType.isInstanceOf[ArrayType])
    assertTrue(new File(s"${tensorizeInParams.workingDir.trainingDataPath}/_SUCCESS").exists())
  }

  /**
   * Test the correctness of hashing transformation
   *
   */
  @Test
  def testHashTransformation(): Unit = {

    val tensorizeInConfig = new File(
      getClass.getClassLoader.getResource(TENSORIZEIN_CONFIG_PATH_VALUE_SAMPLE).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(WORKING_DIRECTORY_HASH_TRANSFORMATION))

    val params = Array(
      INPUT_PATHS_NAME, INPUT_TEXT_FILE_PATHS,
      WORKING_DIRECTORY_NAME, WORKING_DIRECTORY_HASH_TRANSFORMATION,
      TENSORIZEIN_CONFIG_PATH_NAME, tensorizeInConfig
    )

    val dataFrame = session.read.avro(INPUT_TEXT_FILE_PATHS)
    val tensorizeInParams = TensorizeInJobParamsParser.parse(params)

    val dataFrameExtracted = FeatureExtraction.run(dataFrame, tensorizeInParams)
    val result = FeatureTransformation.run(dataFrameExtracted, tensorizeInParams)

    // Check if the actual columns of the result DataFrame match those specified in TensorizeIn parameters
    TestUtil.checkOutputColumns(result, tensorizeInParams)
    TensorizeInJobHelper.saveDataToHDFS(result, tensorizeInParams)

    val records = session.read.avro(tensorizeInParams.workingDir.trainingDataPath).collect()
    records.foreach {
      record: Row => {
        val indicies = record.getAs[Row](FEATURE_WORDS_WIDE_FEATURES_HASH_COL_NAME).getAs[Seq[Long]](SPARSE_VECTOR_INDICES_FIELD_NAME)
        // Make sure ids are distinct
        assertEquals(indicies.length, indicies.distinct.length)
        // Make sure ids are all within the specified bucket size
        assertTrue(indicies.forall(id => id <= 100))

        val hashedWordSeq = record.getAs[Seq[Int]](FEATURE_WORD_SEQ_HASHED_COL_NAME)
        // Make sure the wordSeq_hashed is hashed to 4 Integers
        assertEquals(hashedWordSeq.length, 4)
      }
    }
  }
}