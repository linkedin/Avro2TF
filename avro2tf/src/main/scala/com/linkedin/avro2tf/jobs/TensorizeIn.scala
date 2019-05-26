package com.linkedin.avro2tf.jobs

import com.linkedin.avro2tf.helpers.TensorizeInJobHelper
import com.linkedin.avro2tf.parsers._
import com.linkedin.avro2tf.utils.{Constants, TrainingMode}
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

/**
 * The TensorizeIn job is used to make users training data ready to be consumed by deep learning training frameworks like TensorFlow
 *
 */
object TensorizeIn {
  val logger: Logger = LoggerFactory.getLogger(TensorizeIn.getClass)

  /**
   * The main function to perform TensorizeIn job
   *
   * @param spark Spark Session
   * @param params TensorizeIn parameters specified by user
   */
  def run(spark: SparkSession, params: TensorizeInParams): Unit = {

    // Read input data from HDFS to Spark DataFrame
    var dataFrame = TensorizeInJobHelper.readDataFromHDFS(spark, params)

    // Sanity check on tensor names specified in TensorizeIn config
    TensorizeInJobHelper.tensorsNameCheck(dataFrame, params)

    // Extracts features that will be converted to tensors
    dataFrame = (new FeatureExtraction).run(dataFrame, params)

    // Transforms features that will be converted to tensors
    dataFrame = (new FeatureTransformation).run(dataFrame, params)

    // Generate tensor metadata only in train mode; otherwise, directly load existing ones from working directory
    if (params.executionMode == TrainingMode.training) {
      if (params.enableCache) dataFrame.persist(StorageLevel.MEMORY_AND_DISK_SER)

      // Generate tensor metadata
      (new FeatureListGeneration).run(dataFrame, params)
      (new TensorMetadataGeneration).run(dataFrame, params)
    }

    // Convert indices if not skipped
    if (!params.skipConversion) {
      // Convert String indices to numerical Id indices
      dataFrame = (new FeatureIndicesConversion).run(dataFrame, params)

      // Save input data from Spark DataFrame to HDFS
      TensorizeInJobHelper.saveDataToHDFS(dataFrame, params)
    }
  }

  /**
   * Entry point to run the TensorizeIn Spark job
   *
   * @param args arguments
   */
  def main(args: Array[String]): Unit = {

    val params = TensorizeInJobParamsParser.parse(args)
    val sparkSession = SparkSession.builder().appName(getClass.getName).getOrCreate()

    try {
      run(sparkSession, params)
    } finally {
      sparkSession.stop()
    }
  }

  /**
   * Case class to represent the NameTermValue (NTV)
   *
   * @param name Name of a feature
   * @param term Term of a feature
   * @param value Value of a feature
   */
  case class NameTermValue(name: String, term: String, value: Float)

  /**
   * Case class for SparseVector type
   *
   * @param indices The indices of a sparse vector
   * @param values The values of a sparse vector
   */
  case class SparseVector(indices: Seq[Long], values: Seq[Float])

  /**
   * Case class for IdValue type
   *
   * @param id The mapped id of name+term
   * @param value Value of a feature, can also be weight
   */
  case class IdValue(id: Long, value: Float)

  /**
   * Feature list entry represented by a column name and the value of a feature entry
   *
   * @param columnName Name of the column
   * @param featureEntry Value of a feature entry, e.g. combination of Name + Term or a String
   */
  case class FeatureListEntry(columnName: String, featureEntry: String)
}

