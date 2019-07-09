package com.linkedin.avro2tf.jobs

import com.linkedin.avro2tf.parsers.TensorizeInParams
import com.linkedin.avro2tf.utils.{Constants, HashingUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{expr, udf}

/**
 * Append a partition id according to the specified partition field and num of partitions
 */
object PartitionIdGeneration {

  /**
   * The main function to append partition id
   *
   * @param dataFrame Input data Spark DataFrame
   * @param params TensorizeIn parameters specified by user
   * @return A Spark DataFrame
   */
  def run(dataFrame: DataFrame, params: TensorizeInParams): DataFrame = {

    dataFrame
      .withColumn(
        Constants.PARTITION_ID_FIELD_NAME,
        getPartitionId(params.numPartitions)(expr(params.partitionFieldName)).cast("string"))
  }

  /**
   * Generate a partition id according to the specified partition field and num of partitions
   *
   * @param numPartitions The specified num of partitions
   * @return A UDF to get partition id with string column input type
   */
  private def getPartitionId(numPartitions: Int): UserDefinedFunction = {

    udf {
      id: String => {
        HashingUtils.md5(id, HashingUtils._globalSalt, numPartitions)
      }
    }
  }
}
