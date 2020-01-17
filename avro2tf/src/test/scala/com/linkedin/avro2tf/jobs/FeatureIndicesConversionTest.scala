package com.linkedin.avro2tf.jobs

import java.io.File

import com.databricks.spark.avro._
import com.linkedin.avro2tf.constants.Avro2TFJobParamNames
import com.linkedin.avro2tf.helpers.Avro2TFJobHelper
import com.linkedin.avro2tf.parsers.Avro2TFJobParamsParser
import com.linkedin.avro2tf.utils.ConstantsForTest._
import com.linkedin.avro2tf.utils.{CommonUtils, TestUtil, WithLocalSparkSession}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.testng.Assert._
import org.testng.annotations.{DataProvider, Test}

class FeatureIndicesConversionTest extends WithLocalSparkSession {

  /**
   * Data provider for feature indices conversion test
   *
   */
  @DataProvider
  def testData(): Array[Array[Any]] = {

    Array(
      Array(AVRO_RECORD, false),
      Array(TF_RECORD, true)
    )
  }

  /**
   * Test the correctness of indices conversion job
   */
  @Test(dataProvider = "testData")
  def testConversion(outputFormat: String, discardUnknownEntries: Boolean): Unit = {

    val avro2TFConfig = new File(
      getClass.getClassLoader.getResource(AVRO2TF_CONFIG_PATH_VALUE_SAMPLE).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(WORKING_DIRECTORY_INDICES_CONVERSION))

    val params = Map(
      Avro2TFJobParamNames.INPUT_PATHS -> INPUT_TEXT_FILE_PATHS,
      Avro2TFJobParamNames.WORKING_DIR -> WORKING_DIRECTORY_INDICES_CONVERSION,
      Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> avro2TFConfig,
      Avro2TFJobParamNames.OUTPUT_FORMAT -> outputFormat,
      Avro2TFJobParamNames.DISCARD_UNKNOWN_ENTRIES -> discardUnknownEntries.toString
    )
    val dataFrame = session.read.avro(INPUT_TEXT_FILE_PATHS)
    val avro2TFParams = if (outputFormat == TF_RECORD) {
      Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(params))
    } else {
      Avro2TFJobParamsParser.parse(
        TestUtil.convertParamMapToParamList(
          params ++ Map(Avro2TFJobParamNames.EXTRA_COLUMNS_TO_KEEP -> EXTRA_COLUMNS_TO_KEEP_VALUE)
        )
      )
    }

    val dataFrameExtracted = FeatureExtraction.run(dataFrame, avro2TFParams)
    val dataFrameTransformed = FeatureTransformation.run(dataFrameExtracted, avro2TFParams)
    FeatureListGeneration.run(dataFrameTransformed, avro2TFParams)
    val convertedDataFrame = FeatureIndicesConversion.run(dataFrameTransformed, avro2TFParams)

    TestUtil.checkOutputColumns(convertedDataFrame, avro2TFParams)

    // check if the type of "wordSeq" column is the expected Seq[Long]
    val convertedTextColummType = convertedDataFrame.schema(FEATURE_WORD_SEQ_COL_NAME).dataType
    assertTrue(CommonUtils.isArrayOfLong(convertedTextColummType))
    assertTrue(convertedDataFrame.schema(FEATURE_FIRST_WORD_COL_NAME).dataType.isInstanceOf[LongType])

    // check if the type of "words_wideFeatures_sparse" column is the expected SparseVector type
    val convertedNTVSparseColummType = convertedDataFrame.schema(FEATURE_WORDS_WIDE_FEATURES_SPARSE_COL_NAME).dataType
    assertTrue(convertedNTVSparseColummType.isInstanceOf[StructType])
    val convertedNTVStructType = convertedNTVSparseColummType.asInstanceOf[StructType]
    assertTrue(CommonUtils.isSparseTensor(convertedNTVStructType))
    assertTrue(CommonUtils.isArrayOfLong(convertedNTVStructType(SPARSE_VECTOR_INDICES_FIELD_NAME).dataType))
    assertTrue(CommonUtils.isArrayOfFloat(convertedNTVStructType(SPARSE_VECTOR_VALUES_FIELD_NAME).dataType))

    // check if the type of "words_wideFeatures_dense" column is the expected dense vector type
    val convertedNTVDenseColummType = convertedDataFrame.schema(FEATURE_WORDS_WIDE_FEATURES_DENSE_COL_NAME).dataType
    assertTrue(CommonUtils.isArrayOfFloat(convertedNTVDenseColummType))

    // Make sure the dense and sparse format have the same value at the same index
    convertedDataFrame.foreach {
      row => {
        val sparseVector = row.getAs[Row](FEATURE_WORDS_WIDE_FEATURES_SPARSE_COL_NAME)
        val indices = sparseVector.getAs[Seq[Long]](SPARSE_VECTOR_INDICES_FIELD_NAME)
        val values = sparseVector.getAs[Seq[Float]](SPARSE_VECTOR_VALUES_FIELD_NAME)
        val denseVector = row.getAs[Seq[Float]](FEATURE_WORDS_WIDE_FEATURES_DENSE_COL_NAME)
        indices.zip(values).forall(x => denseVector(x._1.toInt) == x._2)
      }
    }

    Avro2TFJobHelper.saveDataToHDFS(convertedDataFrame, avro2TFParams)
    assertTrue(new File(s"${avro2TFParams.workingDir.trainingDataPath}/_SUCCESS").exists())
  }

  /**
    * Test the correctness of indices conversion job for sequence text input. In the test data, there is:
    * "name" : "jobSegments",
    * "type" : [
    * {
    * "type": "array",
    * "items": "string"
    * }
    * ]
    *
    * The FeatureIndicesConversion should convert it to a varLen of long
    */
  @Test(dataProvider = "testData")
  def testTextSeqConversion(outputFormat: String, discardUnknownEntries: Boolean): Unit = {
    val avro2TFConfig = new File(
      getClass.getClassLoader.getResource(AVRO2TF_CONFIG_PATH_VALUE_TEXT_SEQ).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(WORKING_DIRECTORY_INDICES_CONVERSION))

    val params = Map(
      Avro2TFJobParamNames.INPUT_PATHS -> INPUT_TEXT_SEQ_FILE_PATHS,
      Avro2TFJobParamNames.WORKING_DIR -> WORKING_DIRECTORY_INDICES_CONVERSION,
      Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> avro2TFConfig,
      Avro2TFJobParamNames.OUTPUT_FORMAT -> outputFormat,
      Avro2TFJobParamNames.DISCARD_UNKNOWN_ENTRIES -> discardUnknownEntries.toString
    )
    val dataFrame = session.read.avro(INPUT_TEXT_SEQ_FILE_PATHS)
    val avro2TFParams = Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(params))

    val dataFrameExtracted = FeatureExtraction.run(dataFrame, avro2TFParams)
    val dataFrameTransformed = FeatureTransformation.run(dataFrameExtracted, avro2TFParams)
    FeatureListGeneration.run(dataFrameTransformed, avro2TFParams)
    val convertedDataFrame = FeatureIndicesConversion.run(dataFrameTransformed, avro2TFParams)

    val input = dataFrame.select("jobSegments").collect()
    val output = convertedDataFrame.select("jobSegments_ids").collect()

    assertTrue(input.length == output.length)
    output.zipWithIndex.foreach {
      case (row, index) => {
        val names = input(index).getAs[Seq[String]](0).length
        val ids = row.getAs[Seq[Long]](0).length
        if (discardUnknownEntries) {
          assertTrue(ids <= names)
        } else {
          assertTrue(ids == names)
        }
      }
    }
  }
}