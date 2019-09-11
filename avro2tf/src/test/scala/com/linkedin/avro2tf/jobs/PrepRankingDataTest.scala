package com.linkedin.avro2tf.jobs

import java.io.File

import scala.io.Source

import com.linkedin.avro2tf.constants.{Avro2TFJobParamNames, Constants, PrepRankingJobParamNames}
import com.linkedin.avro2tf.parsers.{Avro2TFJobParamsParser, PrepRankingDataParamsParser}
import com.linkedin.avro2tf.utils.ConstantsForTest._
import com.linkedin.avro2tf.utils.TestUtil.removeWhiteSpace
import org.testng.annotations.Test
import com.linkedin.avro2tf.utils.{TestUtil, WithLocalSparkSession}
import org.apache.commons.io.FileUtils
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
      PrepRankingJobParamNames.GROUP_LIST_MAX_SIZE -> 2,
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
  }
}
