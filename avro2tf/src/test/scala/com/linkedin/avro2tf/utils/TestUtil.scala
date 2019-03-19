package com.linkedin.avro2tf.utils

import com.linkedin.avro2tf.helpers.TensorizeInConfigHelper
import com.linkedin.avro2tf.parsers.TensorizeInParams
import org.apache.spark.sql.DataFrame
import org.testng.Assert.assertEqualsNoOrder

/**
 * Utility file for TensorizeIn related tests
 *
 */
object TestUtil {

  /**
   * Check if the actual columns of a Spark DataFrame match those specified in TensorizeIn parameters
   *
   * @param dataFrame A Spark DataFrame
   * @param tensorizeInParams TensorizeIn parameters specified by users
   */
  def checkOutputColumns(dataFrame: DataFrame, tensorizeInParams: TensorizeInParams): Unit = {

    val actualColumns = dataFrame.columns
    val expectedColumns = TensorizeInConfigHelper.getOutputTensorNames(tensorizeInParams) ++
      tensorizeInParams.extraColumnsToKeep.map {
        columnName => if(columnName.contains(Constants.COLUMN_NAME_ALIAS_DELIMITER)) {
          columnName.trim.split(Constants.COLUMN_NAME_ALIAS_DELIMITER).last
        } else {
          columnName
        }
      }

    assertEqualsNoOrder(actualColumns.toList.toArray, expectedColumns.toArray)
  }
}