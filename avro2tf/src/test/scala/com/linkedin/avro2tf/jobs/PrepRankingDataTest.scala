package com.linkedin.avro2tf.jobs

import java.io.File

import com.linkedin.avro2tf.parsers.{PrepRankingDataParamsParser, TensorizeInJobParamsParser}
import com.linkedin.avro2tf.utils.ConstantsForTest._
import com.linkedin.avro2tf.utils.Constants
import org.testng.annotations.Test
import com.linkedin.avro2tf.utils.WithLocalSparkSession
import org.apache.commons.io.FileUtils
import org.testng.Assert._

class PrepRankingDataTest extends WithLocalSparkSession {

  @Test
  def testPrepRankingData(): Unit = {

    val workingDir = WORKING_DIRECTORY_AVRO2TF_MOVIELENS
    val tensorizeInConfig = new File(
      getClass.getClassLoader.getResource(TENSORIZEIN_CONFIG_PATH_VALUE_MOVIELENS_RANK).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(workingDir))

    val inputPath = INPUT_MOVIELENS_FILE_PATHS
    val trainingParams = Array(
      INPUT_PATHS_NAME, inputPath,
      WORKING_DIRECTORY_NAME, workingDir,
      TENSORIZEIN_CONFIG_PATH_NAME, tensorizeInConfig
    )
    val tensorizeInTrainingParams = TensorizeInJobParamsParser.parse(trainingParams)
    TensorizeIn.run(session, tensorizeInTrainingParams)
    assertTrue(new File(s"${tensorizeInTrainingParams.workingDir.trainingDataPath}/_SUCCESS").exists())
    assertTrue(new File(tensorizeInTrainingParams.workingDir.tensorMetadataPath).exists())
    assertTrue(new File(tensorizeInTrainingParams.workingDir.featureListPath).exists())

    val dataOutputPath = s"$workingDir/rankingOutput"
    val metadataOutputPath = s"$workingDir/rankingMetadataOutput"
    val prepRankingParams = Array(
      "--input-data-path", tensorizeInTrainingParams.workingDir.trainingDataPath,
      "--input-metadata-path", tensorizeInTrainingParams.workingDir.tensorMetadataPath,
      "--output-data-path", dataOutputPath,
      "--output-metadata-path", metadataOutputPath,
      "--group-id-list", "userId",
      "--query-feature-list", "movieId_hashed",
      "--group-list-max-size", "2",
      "--execution-mode", "training",
      "--num-output-files", "1"
    )
    val params = PrepRankingDataParamsParser.parse(prepRankingParams)
    PrepRankingData.run(session, params)

    assertTrue(new File(s"$dataOutputPath/_SUCCESS").exists())
    assertTrue(new File(s"$metadataOutputPath/${Constants.TENSOR_METADATA_FILE_NAME}").exists())
    assertTrue(new File(s"$metadataOutputPath/${Constants.CONTENT_FEATURE_LIST}").exists())
  }
}
