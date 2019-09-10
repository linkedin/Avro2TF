package com.linkedin.avro2tf.jobs

import java.io.{File, FileOutputStream, PrintWriter}

import scala.io.Source

import com.databricks.spark.avro._
import com.linkedin.avro2tf.constants.Avro2TFJobParamNames
import com.linkedin.avro2tf.parsers.Avro2TFJobParamsParser
import com.linkedin.avro2tf.utils.ConstantsForTest._
import com.linkedin.avro2tf.utils.TestUtil.removeWhiteSpace
import com.linkedin.avro2tf.utils.{TestUtil, WithLocalSparkSession}
import org.apache.commons.io.FileUtils
import org.testng.Assert._
import org.testng.annotations.{DataProvider, Test}

class TensorMetadataGenerationTest extends WithLocalSparkSession {

  @DataProvider(name = "testFilenamesProvider")
  def getTestFilenames: Array[Array[Object]] = {

    Array(
      Array(AVRO2TF_CONFIG_PATH_VALUE_SAMPLE, EXPECTED_TENSOR_METADATA_GENERATED_JSON_PATH_TEXT),
      Array(
        AVRO2TF_CONFIG_PATH_VALUE_SAMPLE_WITHOUT_INT_FEATURES,
        EXPECTED_TENSOR_METADATA_WITHOUT_INT_FEATURES_GENERATED_JSON_PATH_TEXT
      )
    )
  }

  /**
   * Test if the Tensor Metadata Generation job can finish successfully
   */
  @Test(dataProvider = "testFilenamesProvider")
  def testTensorMetadataGeneration(avro2TFConfigPath: String, expectedTensorMetadataPath: String): Unit = {

    val avro2TFConfigFile = new File(
      getClass.getClassLoader.getResource(avro2TFConfigPath).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(WORKING_DIRECTORY_TENSOR_METADATA_GENERATION_TEXT))

    // Set up external feature list
    val externalFeatureListFullPath = s"$WORKING_DIRECTORY_TENSOR_METADATA_GENERATION_TEXT/$EXTERNAL_FEATURE_LIST_PATH_TEXT"
    new File(externalFeatureListFullPath).mkdirs()
    new PrintWriter(
      new FileOutputStream(
        s"$externalFeatureListFullPath/$EXTERNAL_FEATURE_LIST_FILE_NAME_TEXT",
        ENABLE_APPEND)) {
      write(SAMPLE_EXTERNAL_FEATURE_LIST)
      close()
    }

    val params = Map(
      Avro2TFJobParamNames.INPUT_PATHS -> INPUT_TEXT_FILE_PATHS,
      Avro2TFJobParamNames.WORKING_DIR -> WORKING_DIRECTORY_TENSOR_METADATA_GENERATION_TEXT,
      Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> avro2TFConfigFile,
      Avro2TFJobParamNames.EXTERNAL_FEATURE_LIST_PATH -> externalFeatureListFullPath
    )

    val dataFrame = session.read.avro(INPUT_TEXT_FILE_PATHS)
    val avro2TFParams = Avro2TFJobParamsParser.parse(TestUtil.convertParamMapToParamList(params))

    val dataFrameExtracted = FeatureExtraction.run(dataFrame, avro2TFParams)
    val dataFrameTransformed = FeatureTransformation.run(dataFrameExtracted, avro2TFParams)
    FeatureListGeneration.run(dataFrameTransformed, avro2TFParams)
    TensorMetadataGeneration.run(dataFrameTransformed, avro2TFParams)

    // Check if tensor metadata JSON file is correctly generated
    val expectedTensorMetadata = getClass.getClassLoader.getResource(expectedTensorMetadataPath).getFile
    assertEquals(
      removeWhiteSpace(Source.fromFile(avro2TFParams.workingDir.tensorMetadataPath).mkString),
      removeWhiteSpace(Source.fromFile(expectedTensorMetadata).mkString)
    )
  }
}