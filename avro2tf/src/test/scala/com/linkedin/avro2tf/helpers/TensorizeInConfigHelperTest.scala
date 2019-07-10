package com.linkedin.avro2tf.helpers

import java.io.File

import com.linkedin.avro2tf.parsers.TensorizeInJobParamsParser
import org.testng.annotations.Test
import com.linkedin.avro2tf.utils.ConstantsForTest._
import org.testng.Assert._

class TensorizeInConfigHelperTest {

  val movieLensConfigPath: String = new File(
    getClass.getClassLoader.getResource(TENSORIZEIN_CONFIG_PATH_VALUE_MOVIELENS).getFile
  ).getAbsolutePath

  @Test
  def testGetColsWithHashInfo() {

    val commandLineArgs = Seq(
      INPUT_PATHS_NAME, INPUT_PATHS_VALUE,
      WORKING_DIRECTORY_NAME, WORKING_DIRECTORY_VALUE,
      INPUT_DATE_RANGE_NAME, INPUT_DATE_RANGE_VALUE,
      NUM_OUTPUT_FILES_NAME, NUM_OUTPUT_FILES_VALUE,
      MIN_PARTS_NAME, MIN_PARTS_VALUE,
      ENABLE_SHUFFLE_NAME, ENABLE_SHUFFLE_VALUE,
      EXTERNAL_FEATURE_LIST_PATH_NAME, EXTERNAL_FEATURE_LIST_PATH_VALUE,
      TENSORIZEIN_CONFIG_PATH_NAME, movieLensConfigPath,
      ENABLE_TRAIN_MODE_NAME, ENABLE_TRAIN_MODE_VALUE
    )

    val params = TensorizeInJobParamsParser.parse(commandLineArgs)

    val colsWithHashInfo = TensorizeInConfigHelper.getColsWithHashInfo(params)

    assertFalse(colsWithHashInfo.contains("userId"))
    assertTrue(colsWithHashInfo.contains("genreFeatures_movieLatentFactorFeatures_response"))
  }
}