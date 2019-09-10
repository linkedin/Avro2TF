package com.linkedin.avro2tf.utils

import com.linkedin.avro2tf.constants.Constants
import com.linkedin.avro2tf.helpers.Avro2TFConfigHelper
import com.linkedin.avro2tf.parsers.Avro2TFParams
import org.apache.spark.sql.DataFrame
import org.testng.Assert.assertEqualsNoOrder

/**
 * Utility file for Avro2T related tests
 *
 */
object TestUtil {

  /**
   * Check if the actual columns of a Spark DataFrame match those specified in Avro2TF parameters
   *
   * @param dataFrame A Spark DataFrame
   * @param avro2TFParams Avro2TF parameters specified by users
   */
  def checkOutputColumns(dataFrame: DataFrame, avro2TFParams: Avro2TFParams): Unit = {

    val actualColumns = dataFrame.columns
    val expectedColumns = Avro2TFConfigHelper.getOutputTensorNames(avro2TFParams) ++
      avro2TFParams.extraColumnsToKeep.map {
        columnName =>
          if (columnName.contains(Constants.COLUMN_NAME_ALIAS_DELIMITER)) {
            columnName.trim.split(Constants.COLUMN_NAME_ALIAS_DELIMITER).last
          } else {
            columnName
          }
      }

    assertEqualsNoOrder(actualColumns.toList.toArray, expectedColumns.toArray)
  }

  /**
   * Remove the whitespace from a string.
   *
   * @param s - the string
   * @return the string with whitespaces removed
   */
  def removeWhiteSpace(s: String): String = s.replaceAll("\\s", "")

  /**
   * Convert a map of params to a list of params with param name prefix added
   *
   * @param paramMap A map of param name to param value
   * @return A flattened list of param name and param value
   */
  def convertParamMapToParamList(paramMap: Map[String, Any]): Seq[String] = {

    paramMap.flatMap(x => Seq(s"--${x._1}", x._2.toString)).toSeq
  }
}