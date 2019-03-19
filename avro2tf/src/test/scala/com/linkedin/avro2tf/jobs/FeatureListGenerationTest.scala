package com.linkedin.avro2tf.jobs

import java.io.{File, FileOutputStream, PrintWriter}

import scala.collection.mutable

import com.databricks.spark.avro._
import com.linkedin.avro2tf.parsers.TensorizeInJobParamsParser
import com.linkedin.avro2tf.utils.Constants.{HASH_INFO, NTV_NAME, NTV_TERM, NTV_VALUE}
import com.linkedin.avro2tf.utils.ConstantsForTest._
import com.linkedin.avro2tf.utils.WithLocalSparkSession

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.testng.Assert._
import org.testng.annotations.Test

class FeatureListGenerationTest extends WithLocalSparkSession {

  /**
   * Test if the Feature List Generation job can finish successfully
   *
   */
  @Test
  def testFeatureList(): Unit = {

    val tensorizeInConfig = new File(
      getClass.getClassLoader.getResource(TENSORIZEIN_CONFIG_PATH_VALUE_SAMPLE).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(WORKING_DIRECTORY_FEATURE_LIST_GENERATION_TEXT))

    // Set up external feature list
    val externalFeatureListFullPath = s"$WORKING_DIRECTORY_FEATURE_LIST_GENERATION_TEXT/$EXTERNAL_FEATURE_LIST_PATH_TEXT"
    new File(externalFeatureListFullPath).mkdirs()
    new PrintWriter(new FileOutputStream(s"$externalFeatureListFullPath/$EXTERNAL_FEATURE_LIST_FILE_NAME_TEXT", false)) {
      write(SAMPLE_EXTERNAL_FEATURE_LIST)
      close()
    }

    val params = Array(
      INPUT_PATHS_NAME, INPUT_TEXT_FILE_PATHS,
      WORKING_DIRECTORY_NAME, WORKING_DIRECTORY_FEATURE_LIST_GENERATION_TEXT,
      TENSORIZEIN_CONFIG_PATH_NAME, tensorizeInConfig,
      EXTERNAL_FEATURE_LIST_PATH_NAME, externalFeatureListFullPath
    )

    val dataFrame = session.read.avro(INPUT_TEXT_FILE_PATHS)
    val tensorizeInParams = TensorizeInJobParamsParser.parse(params)

    val dataFrameExtracted = (new FeatureExtraction).run(dataFrame, tensorizeInParams)
    val dataFrameTransformed = (new FeatureTransformation).run(dataFrameExtracted, tensorizeInParams)
    (new FeatureListGeneration).run(dataFrameTransformed, tensorizeInParams)

    // Check if columns of String Array type and NTV struct type have feature lists generated
    val fileSystem = FileSystem.get(session.sparkContext.hadoopConfiguration)
    val featureListPath = new Path(tensorizeInParams.workingDir.featureListPath)

    val filesIterator = fileSystem.listFiles(featureListPath, ENABLE_RECURSIVE)
    val files = new mutable.HashSet[String]
    while (filesIterator.hasNext) {
      files.add(filesIterator.next().getPath.getName)
    }
    fileSystem.close()

    val colsNeedFeatureList = tensorizeInParams.tensorizeInConfig.features
      .filter(feature => {
        val colName = feature.outputTensorInfo.name
        dataFrameTransformed.schema(colName).dataType match {
          case arrayType: ArrayType if arrayType.elementType.isInstanceOf[StringType] => true
          case arrayType: ArrayType if arrayType.elementType.isInstanceOf[StructType] =>
            val ntvType = arrayType.elementType.asInstanceOf[StructType]
            ntvType.fieldNames.contains(NTV_NAME) && ntvType.fieldNames.contains(NTV_TERM) && ntvType.fieldNames.contains(NTV_VALUE)
          case _: StringType => true
          case _ => false
        }
      })
      .filter(feature => {
        feature.inputFeatureInfo.get.transformConfig match {
          case Some(config) if config.contains(HASH_INFO) => false
          case _ => true
        }
      })
      .map(feature => feature.outputTensorInfo.name)
      .toSet

    assertTrue(colsNeedFeatureList.subsetOf(files))
  }
}