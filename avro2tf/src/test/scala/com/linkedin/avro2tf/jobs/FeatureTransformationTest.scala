package com.linkedin.avro2tf.jobs

import java.io.File

import com.databricks.spark.avro._
import com.linkedin.avro2tf.constants.Avro2TFJobParamNames
import com.linkedin.avro2tf.helpers.Avro2TFJobHelper
import com.linkedin.avro2tf.parsers.Avro2TFJobParamsParser
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

    val avro2TFConfig = new File(
      getClass.getClassLoader.getResource(AVRO2TF_CONFIG_PATH_VALUE_SAMPLE).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(WORKING_DIRECTORY_FEATURE_TRANSFORMATION_TEXT))

    val params = Map(
      Avro2TFJobParamNames.INPUT_PATHS -> INPUT_TEXT_FILE_PATHS,
      Avro2TFJobParamNames.WORKING_DIR -> WORKING_DIRECTORY_FEATURE_TRANSFORMATION_TEXT,
      Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> avro2TFConfig
    )

    val dataFrame = session.read.avro(INPUT_TEXT_FILE_PATHS)
    val avro2TFParams = Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(params))

    val dataFrameExtracted = FeatureExtraction.run(dataFrame, avro2TFParams)
    val result = FeatureTransformation.run(dataFrameExtracted, avro2TFParams)

    Avro2TFJobHelper.saveDataToHDFS(result, avro2TFParams)

    // Check if the actual columns of the result DataFrame match those specified in Avro2TF parameters
    TestUtil.checkOutputColumns(result, avro2TFParams)
    // Check text feature is successfully transformed to Strings
    assertTrue(result.schema(FEATURE_WORD_SEQ_COL_NAME).dataType.isInstanceOf[ArrayType])
    assertTrue(new File(s"${avro2TFParams.workingDir.trainingDataPath}/_SUCCESS").exists())
  }

  /**
   * Test the correctness of hashing transformation
   *
   */
  @Test
  def testHashTransformation(): Unit = {

    val avro2TFConfig = new File(
      getClass.getClassLoader.getResource(AVRO2TF_CONFIG_PATH_VALUE_SAMPLE).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(WORKING_DIRECTORY_HASH_TRANSFORMATION))

    val params = Map(
      Avro2TFJobParamNames.INPUT_PATHS -> INPUT_TEXT_FILE_PATHS,
      Avro2TFJobParamNames.WORKING_DIR -> WORKING_DIRECTORY_HASH_TRANSFORMATION,
      Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> avro2TFConfig
    )

    val dataFrame = session.read.avro(INPUT_TEXT_FILE_PATHS)
    val avro2TFParams = Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(params))

    val dataFrameExtracted = FeatureExtraction.run(dataFrame, avro2TFParams)
    val result = FeatureTransformation.run(dataFrameExtracted, avro2TFParams)

    // Check if the actual columns of the result DataFrame match those specified in Avro2TF parameters
    TestUtil.checkOutputColumns(result, avro2TFParams)
    Avro2TFJobHelper.saveDataToHDFS(result, avro2TFParams)

    val records = session.read.avro(avro2TFParams.workingDir.trainingDataPath).collect()
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