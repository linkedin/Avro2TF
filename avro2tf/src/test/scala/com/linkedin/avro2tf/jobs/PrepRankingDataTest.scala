package com.linkedin.avro2tf.jobs

import java.io.File

import scala.io.Source
import com.linkedin.avro2tf.constants.{Avro2TFJobParamNames, Constants, PrepRankingJobParamNames}
import com.linkedin.avro2tf.parsers.{Avro2TFJobParamsParser, PrepRankingDataParamsParser}
import com.linkedin.avro2tf.utils.ConstantsForTest._
import com.linkedin.avro2tf.utils.TestUtil.removeWhiteSpace
import org.testng.annotations.Test
import com.linkedin.avro2tf.utils.{IOUtils, TestUtil, WithLocalSparkSession}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Row}
import org.testng.Assert._

class PrepRankingDataTest extends WithLocalSparkSession {

  @Test
  def testPrepRankingData(): Unit = {

    val workingDir = WORKING_DIRECTORY_AVRO2TF_MOVIELENS
    val avro2TFConfig = new File(
      getClass.getClassLoader.getResource(AVRO2TF_CONFIG_PATH_VALUE_MOVIELENS_RANK).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(workingDir))

    val inputPath = INPUT_MOVIELENS_FILE_PATHS
    val trainingParams = Map(
      Avro2TFJobParamNames.INPUT_PATHS -> inputPath,
      Avro2TFJobParamNames.WORKING_DIR -> workingDir,
      Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> avro2TFConfig
    )
    val avro2TFTrainingParams = Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(trainingParams))
    Avro2TF.run(session, avro2TFTrainingParams)
    assertTrue(new File(s"${avro2TFTrainingParams.workingDir.trainingDataPath}/_SUCCESS").exists())
    assertTrue(new File(avro2TFTrainingParams.workingDir.tensorMetadataPath).exists())
    assertTrue(new File(avro2TFTrainingParams.workingDir.featureListPath).exists())

    val dataOutputPath = s"$workingDir/rankingOutput"
    val metadataOutputPath = s"$workingDir/rankingMetadataOutput"
    val prepRankingParams = Map(
      PrepRankingJobParamNames.INPUT_DATA_PATH -> avro2TFTrainingParams.workingDir.trainingDataPath,
      PrepRankingJobParamNames.INPUT_METADATA_PATH -> avro2TFTrainingParams.workingDir.tensorMetadataPath,
      PrepRankingJobParamNames.OUTPUT_DATA_PATH -> dataOutputPath,
      PrepRankingJobParamNames.OUTPUT_METADATA_PATH -> metadataOutputPath,
      PrepRankingJobParamNames.GROUP_ID_LIST -> "userId",
      PrepRankingJobParamNames.GROUP_LIST_MAX_SIZE -> 10,
      PrepRankingJobParamNames.EXECUTION_MODE -> "training",
      PrepRankingJobParamNames.NUM_OUTPUT_FILES -> 1
    )
    val params = PrepRankingDataParamsParser.parse(TestUtil.convertParamMapToParamList(prepRankingParams))
    PrepRankingData.run(session, params)

    val outputMetadataFile = s"$metadataOutputPath/${Constants.TENSOR_METADATA_FILE_NAME}"
    assertTrue(new File(s"$dataOutputPath/_SUCCESS").exists())
    assertTrue(new File(outputMetadataFile).exists())
    assertTrue(new File(s"$metadataOutputPath/${Constants.CONTENT_FEATURE_LIST}").exists())

    // Check if tensor metadata JSON file is correctly generated
    val expectedTensorMetadata = getClass.getClassLoader.getResource(EXPECTED_TENSOR_METADATA_MOVIELENS_RANK).getFile
    assertEquals(
      removeWhiteSpace(Source.fromFile(outputMetadataFile).mkString),
      removeWhiteSpace(Source.fromFile(expectedTensorMetadata).mkString)
    )

    val df = IOUtils.readAvro(session, params.outputDataPath)
    // verify sparse feature schema is correct
    validateSparseFeatureOutputSchema(df, Seq("genreFeatures", "genreFeatures_movieLatentFactorFeatures", "genreFeatures_movieLatentFactorFeatures_response"))
  }

  /**
    * For 2D Sparse feature schema is similar as https://www.tensorflow.org/api_docs/python/tf/io/SparseFeature, which has column major indices:
    * indices0: longArray
    * indices1: longArray
    * values: floatArray
    * This method is to validate the output schema is expected
    */
  def validateSparseFeatureOutputSchema(df: DataFrame, sparseFeatures: Seq[String]) = {
    sparseFeatures.foreach {
      feature =>
        df.select(feature).collect().foreach {
          row =>
            val indices0 = row.getAs[Row](0).getAs[Seq[Long]]("indices0")
            val indices1 = row.getAs[Row](0).getAs[Seq[Long]]("indices1")
            val values = row.getAs[Row](0).getAs[Seq[Float]]("values")
            assertEquals(indices0.length, indices1.length)
            assertEquals(values.length, indices1.length)
        }
    }
  }

  /**
    * A unit test to test with skip padding, the output data should not pad and the generated metadata should be the same
    */
  @Test
  def testPrepRankingDataWithSkipPadding(): Unit = {

    val workingDir = WORKING_DIRECTORY_AVRO2TF_MOVIELENS
    val avro2TFConfig = new File(
      getClass.getClassLoader.getResource(AVRO2TF_CONFIG_PATH_VALUE_MOVIELENS_RANK).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(workingDir))

    val inputPath = INPUT_MOVIELENS_FILE_PATHS
    val trainingParams = Map(
      Avro2TFJobParamNames.INPUT_PATHS -> inputPath,
      Avro2TFJobParamNames.WORKING_DIR -> workingDir,
      Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> avro2TFConfig
    )
    val avro2TFTrainingParams = Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(trainingParams))
    Avro2TF.run(session, avro2TFTrainingParams)
    assertTrue(new File(s"${avro2TFTrainingParams.workingDir.trainingDataPath}/_SUCCESS").exists())
    assertTrue(new File(avro2TFTrainingParams.workingDir.tensorMetadataPath).exists())
    assertTrue(new File(avro2TFTrainingParams.workingDir.featureListPath).exists())

    val dataOutputPath = s"$workingDir/rankingOutput"
    val metadataOutputPath = s"$workingDir/rankingMetadataOutput"
    val prepRankingParams = Map(
      PrepRankingJobParamNames.INPUT_DATA_PATH -> avro2TFTrainingParams.workingDir.trainingDataPath,
      PrepRankingJobParamNames.INPUT_METADATA_PATH -> avro2TFTrainingParams.workingDir.tensorMetadataPath,
      PrepRankingJobParamNames.OUTPUT_DATA_PATH -> dataOutputPath,
      PrepRankingJobParamNames.OUTPUT_METADATA_PATH -> metadataOutputPath,
      PrepRankingJobParamNames.GROUP_ID_LIST -> "userId",
      PrepRankingJobParamNames.GROUP_LIST_MAX_SIZE -> 10,
      PrepRankingJobParamNames.EXECUTION_MODE -> "training",
      PrepRankingJobParamNames.NUM_OUTPUT_FILES -> 1,
      PrepRankingJobParamNames.SKIP_PADDING -> "true"
    )
    val params = PrepRankingDataParamsParser.parse(TestUtil.convertParamMapToParamList(prepRankingParams))
    PrepRankingData.run(session, params)

    val outputMetadataFile = s"$metadataOutputPath/${Constants.TENSOR_METADATA_FILE_NAME}"
    assertTrue(new File(s"$dataOutputPath/_SUCCESS").exists())
    assertTrue(new File(outputMetadataFile).exists())
    assertTrue(new File(s"$metadataOutputPath/${Constants.CONTENT_FEATURE_LIST}").exists())

    // Check if tensor metadata JSON file is correctly generated
    val expectedTensorMetadata = getClass.getClassLoader.getResource(EXPECTED_TENSOR_METADATA_MOVIELENS_RANK).getFile
    assertEquals(
      removeWhiteSpace(Source.fromFile(outputMetadataFile).mkString),
      removeWhiteSpace(Source.fromFile(expectedTensorMetadata).mkString)
    )

    val df = IOUtils.readAvro(session, params.outputDataPath)
    // verify padding is skipped
    assertTrue(df.select("response").collect().exists(row => row.getAs[Seq[Float]](0).size < 10))
  }

  /**
    * A unit test to test N dimensional dense and sparse Tensor input.
    * The schema for N dimensional dense Tensor is the nested array.
    * The schema for N dimensional sparse Tensor is
    *   indices0: Array[Long]
    *   indices1: Array[Long]
    *   ...
    *   indicesN: Array[Long]
    *   values: Array[Float]
    */
  @Test
  def testPrepRankingDataWithNDimTensor(): Unit = {
    val workingDir = WORKING_DIRECTORY_AVRO2TF_FDS
    FileUtils.deleteDirectory(new File(workingDir))

    val dataOutputPath = s"$workingDir/rankingOutput"
    val metadataOutputPath = s"$workingDir/rankingMetadataOutput"
    val prepRankingParams = Map(
      PrepRankingJobParamNames.INPUT_DATA_PATH -> INPUT_FDS_PATH,
      PrepRankingJobParamNames.INPUT_METADATA_PATH -> METADATA_PATH_FDS,
      PrepRankingJobParamNames.OUTPUT_DATA_PATH -> dataOutputPath,
      PrepRankingJobParamNames.OUTPUT_METADATA_PATH -> metadataOutputPath,
      PrepRankingJobParamNames.GROUP_ID_LIST -> "uid",
      PrepRankingJobParamNames.GROUP_LIST_MAX_SIZE -> 10,
      PrepRankingJobParamNames.EXECUTION_MODE -> "training",
      PrepRankingJobParamNames.NUM_OUTPUT_FILES -> 1
    )
    val params = PrepRankingDataParamsParser.parse(TestUtil.convertParamMapToParamList(prepRankingParams))
    PrepRankingData.run(session, params)

    val outputMetadataFile = s"$metadataOutputPath/${Constants.TENSOR_METADATA_FILE_NAME}"
    assertTrue(new File(s"$dataOutputPath/_SUCCESS").exists())
    assertTrue(new File(outputMetadataFile).exists())
    assertTrue(new File(s"$metadataOutputPath/${Constants.CONTENT_FEATURE_LIST}").exists())

    // Check if tensor metadata JSON file is correctly generated
    val expectedTensorMetadata = getClass.getClassLoader.getResource(EXPECTED_TENSOR_METADATA_FDS_RANK).getFile
    assertEquals(
      removeWhiteSpace(Source.fromFile(outputMetadataFile).mkString),
      removeWhiteSpace(Source.fromFile(expectedTensorMetadata).mkString)
    )
  }
}
