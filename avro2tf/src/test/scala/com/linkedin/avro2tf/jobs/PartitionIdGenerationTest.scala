package com.linkedin.avro2tf.jobs

import java.io.File

import scala.io.Source

import com.databricks.spark.avro._
import com.linkedin.avro2tf.constants.{Avro2TFJobParamNames, Constants}
import com.linkedin.avro2tf.helpers.Avro2TFJobHelper
import com.linkedin.avro2tf.parsers.Avro2TFJobParamsParser
import com.linkedin.avro2tf.utils.ConstantsForTest._
import com.linkedin.avro2tf.utils.TestUtil.removeWhiteSpace
import com.linkedin.avro2tf.utils.{TestUtil, WithLocalSparkSession}
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

    val avro2TFConfig = new File(
      getClass.getClassLoader.getResource(AVRO2TF_CONFIG_PATH_VALUE_SHARE_FEATURE).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(WORKING_DIRECTORY_PARTITION_TEST))

    val params = Map(
      Avro2TFJobParamNames.INPUT_PATHS -> INPUT_SHARE_FEATURE_PATH,
      Avro2TFJobParamNames.WORKING_DIR -> WORKING_DIRECTORY_PARTITION_TEST,
      Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> avro2TFConfig,
      Avro2TFJobParamNames.OUTPUT_FORMAT -> AVRO_RECORD,
      Avro2TFJobParamNames.PARTITION_FIELD_NAME -> "send_platform"
    )
    val dataFrame = session.read.avro(INPUT_SHARE_FEATURE_PATH)
    val avro2TFParams = Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(params))

    val dataFrameExtracted = FeatureExtraction.run(dataFrame, avro2TFParams)
    val dataFrameTransformed = FeatureTransformation.run(dataFrameExtracted, avro2TFParams)
    FeatureListGeneration.run(dataFrameTransformed, avro2TFParams)
    TensorMetadataGeneration.run(dataFrameTransformed, avro2TFParams)
    val convertedDataFrame = FeatureIndicesConversion.run(dataFrameTransformed, avro2TFParams)
    val dataFrameWithPartitionId = PartitionIdGeneration.run(convertedDataFrame, avro2TFParams)

    // Check if the partition id is in final DataFrame
    assertEquals(
      dataFrameWithPartitionId.columns.toSeq.diff(convertedDataFrame.columns.toSeq),
      Seq(Constants.PARTITION_ID_FIELD_NAME))

    // Check the appended partition id is in the metadata
    val expectedTensorMetadataPath = getClass.getClassLoader.getResource(EXPECTED_TENSOR_METADATA_WITH_PARTITION_ID)
      .getFile
    assertEquals(
      removeWhiteSpace(Source.fromFile(avro2TFParams.workingDir.tensorMetadataPath).mkString),
      removeWhiteSpace(Source.fromFile(expectedTensorMetadataPath).mkString))

    Avro2TFJobHelper.saveDataToHDFS(dataFrameWithPartitionId, avro2TFParams)
    val fileSystem = FileSystem.get(dataFrame.sparkSession.sparkContext.hadoopConfiguration)
    val partitionDirs = fileSystem.listStatus(new Path(avro2TFParams.workingDir.trainingDataPath))
      .filter(_.isDirectory()).map(_.getPath.getName).toSeq

    // Check if the the outputs are divided into sub dirs
    assertTrue(partitionDirs.forall(_.matches(s"${Constants.PARTITION_COLUMN_NAME}=[0-9]+")))
  }
}
