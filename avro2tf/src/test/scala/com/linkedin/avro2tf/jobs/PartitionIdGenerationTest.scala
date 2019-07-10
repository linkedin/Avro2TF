package com.linkedin.avro2tf.jobs

import java.io.File

import scala.io.Source

import com.databricks.spark.avro._
import com.linkedin.avro2tf.helpers.TensorizeInJobHelper
import com.linkedin.avro2tf.parsers.TensorizeInJobParamsParser
import com.linkedin.avro2tf.utils.ConstantsForTest._
import com.linkedin.avro2tf.utils.TestUtil.removeWhiteSpace
import com.linkedin.avro2tf.utils.{Constants, WithLocalSparkSession}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.testng.Assert._
import org.testng.annotations.Test

class PartitionIdGenerationTest extends WithLocalSparkSession {

  /**
   * Test the correctness of partition id generation job
   */
  @Test
  def testConversion(): Unit = {

    val tensorizeInConfig = new File(
      getClass.getClassLoader.getResource(TENSORIZEIN_CONFIG_PATH_VALUE_SHARE_FEATURE).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(WORKING_DIRECTORY_PARTITION_TEST))

    val params = Array(
      INPUT_PATHS_NAME, INPUT_SHARE_FEATURE_PATH,
      WORKING_DIRECTORY_NAME, WORKING_DIRECTORY_PARTITION_TEST,
      TENSORIZEIN_CONFIG_PATH_NAME, tensorizeInConfig,
      OUTPUT_FORMAT_NAME, AVRO_RECORD,
      PARTITION_FIELD_NAME, "send_platform"
    )
    val dataFrame = session.read.avro(INPUT_SHARE_FEATURE_PATH)
    val tensorizeInParams = TensorizeInJobParamsParser.parse(params)

    val dataFrameExtracted = (new FeatureExtraction).run(dataFrame, tensorizeInParams)
    val dataFrameTransformed = (new FeatureTransformation).run(dataFrameExtracted, tensorizeInParams)
    (new FeatureListGeneration).run(dataFrameTransformed, tensorizeInParams)
    (new TensorMetadataGeneration).run(dataFrameTransformed, tensorizeInParams)
    val convertedDataFrame = (new FeatureIndicesConversion).run(dataFrameTransformed, tensorizeInParams)
    val dataFrameWithPartitionId = PartitionIdGeneration.run(convertedDataFrame, tensorizeInParams)

    // Check if the partition id is in final DataFrame
    assertEquals(
      dataFrameWithPartitionId.columns.toSeq.diff(convertedDataFrame.columns.toSeq),
      Seq(Constants.PARTITION_ID_FIELD_NAME))

    // Check the appended partition id is in the metadata
    val expectedTensorMetadataPath = getClass.getClassLoader.getResource(EXPECTED_TENSOR_METADATA_WITH_PARTITION_ID)
      .getFile
    assertEquals(
      removeWhiteSpace(Source.fromFile(tensorizeInParams.workingDir.tensorMetadataPath).mkString),
      removeWhiteSpace(Source.fromFile(expectedTensorMetadataPath).mkString))

    TensorizeInJobHelper.saveDataToHDFS(dataFrameWithPartitionId, tensorizeInParams)
    val fileSystem = FileSystem.get(dataFrame.sparkSession.sparkContext.hadoopConfiguration)
    val partitionDirs = fileSystem.listStatus(new Path(tensorizeInParams.workingDir.trainingDataPath))
      .filter(_.isDirectory()).map(_.getPath.getName).toSeq

    // Check if the the outputs are divided into sub dirs
    assertTrue(partitionDirs.forall(_.matches(s"${Constants.PARTITION_ID_FIELD_NAME}=[0-9]+")))
  }
}
