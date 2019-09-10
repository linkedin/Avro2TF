package com.linkedin.avro2tf.helpers

import java.io.File

import com.linkedin.avro2tf.constants.Avro2TFJobParamNames
import com.linkedin.avro2tf.parsers.Avro2TFJobParamsParser
import org.testng.annotations.Test
import com.linkedin.avro2tf.utils.ConstantsForTest._
import com.linkedin.avro2tf.utils.TestUtil.convertParamMapToParamList
import org.testng.Assert._

class Avro2TFConfigHelperTest {

  @Test
  def testGetColsWithHashInfo() {

    val movieLensConfigPath: String = new File(
      getClass.getClassLoader.getResource(AVRO2TF_CONFIG_PATH_VALUE_MOVIELENS).getFile
    ).getAbsolutePath

    val commandLineArgs = Map(
      Avro2TFJobParamNames.INPUT_PATHS -> INPUT_PATHS_VALUE,
      Avro2TFJobParamNames.WORKING_DIR -> WORKING_DIRECTORY_VALUE,
      Avro2TFJobParamNames.INPUT_DATE_RANGE -> INPUT_DATE_RANGE_VALUE,
      Avro2TFJobParamNames.NUM_OUTPUT_FILES -> NUM_OUTPUT_FILES_VALUE,
      Avro2TFJobParamNames.MIN_PARTS -> MIN_PARTS_VALUE,
      Avro2TFJobParamNames.SHUFFLE -> ENABLE_SHUFFLE_VALUE,
      Avro2TFJobParamNames.EXTERNAL_FEATURE_LIST_PATH -> EXTERNAL_FEATURE_LIST_PATH_VALUE,
      Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> movieLensConfigPath
    )

    val params = Avro2TFJobParamsParser.parse(convertParamMapToParamList(commandLineArgs))

    val colsWithHashInfo = Avro2TFConfigHelper.getColsWithHashInfo(params)

    assertFalse(colsWithHashInfo.contains("userId"))
    assertTrue(colsWithHashInfo.contains("genreFeatures_movieLatentFactorFeatures_response"))
  }
}