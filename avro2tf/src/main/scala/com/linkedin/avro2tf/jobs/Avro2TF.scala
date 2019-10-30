package com.linkedin.avro2tf.jobs

import com.linkedin.avro2tf.helpers.Avro2TFJobHelper
import com.linkedin.avro2tf.parsers._
import com.linkedin.avro2tf.utils.TrainingMode
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

/**
 * The Avro2TF job is used to make users training data ready to be consumed by deep learning training frameworks like TensorFlow
 *
 */
object Avro2TF {
  val logger: Logger = LoggerFactory.getLogger(Avro2TF.getClass)

  /**
   * The main function to perform Avro2TF job
   *
   * @param spark Spark Session
   * @param params Avro2TF parameters specified by user
   */
  def run(spark: SparkSession, params: Avro2TFParams): Unit = {

    // Read input data from HDFS to Spark DataFrame
    var dataFrame = Avro2TFJobHelper.readDataFromHDFS(spark, params)

    // Sanity check on tensor names specified in Avro2TF config
    Avro2TFJobHelper.tensorsNameCheck(dataFrame, params)

    // Extracts features that will be converted to tensors
    dataFrame = FeatureExtraction.run(dataFrame, params)

    // Transforms features that will be converted to tensors
    dataFrame = FeatureTransformation.run(dataFrame, params)

    // Generate tensor metadata only in train mode; otherwise, directly load existing ones from working directory
    if (params.executionMode == TrainingMode.training) {
      if (params.enableCache) dataFrame.persist(StorageLevel.MEMORY_AND_DISK_SER)

      // Generate tensor metadata
      FeatureListGeneration.run(dataFrame, params)
      TensorMetadataGeneration.run(dataFrame, params)
    }

    // Convert indices if not skipped
    if (!params.skipConversion) {
      // Convert String indices to numerical Id indices
      dataFrame = FeatureIndicesConversion.run(dataFrame, params)
      if (params.partitionFieldName.nonEmpty) {
        dataFrame = PartitionIdGeneration.run(dataFrame, params)
      }
      Avro2TFJobHelper.saveDataToHDFS(dataFrame, params)
    }
  }

  /**
   * Entry point to run the Avro2TF Spark job
   *
   * @param args arguments
   */
  def main(args: Array[String]): Unit = {

    val params = Avro2TFJobParamsParser.parse(args)
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
   * Companion object of [[SparseVector]]. It takes in extra flag filterZero. If it's set,
   * it will filter out all zeros values and their corresponding indices
   */
  object SparseVector {
    /**
     * To construct a [[SparseVector]] based on user input
     *
     * @param indices indices for constructed [[SparseVector]]
     * @param values values for constructed [[SparseVector]]. It can have zeros if filterZero is false
     * @param filterZero it's a flag to set if filtering out zeros values and their corresponding indices
     */
    def apply(indices: Seq[Long], values: Seq[Float], filterZero: Boolean): SparseVector = {

      if (filterZero) filterOutZeroValues(indices, values) else SparseVector(indices, values)
    }

    private def filterOutZeroValues(indices: Seq[Long], values: Seq[Float]): SparseVector = {

      val (newIndices, newValues) = indices.zip(values)
        .filter { case (_, value) => value != 0.0 }
        .unzip
      SparseVector(newIndices, newValues)
    }
  }

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
   * @param featureEntry Value of a feature entry, e.g. combination of Name + Term or a String; or Value of a feature
   * entry with count of the corresponding (columnName, Value of a feature entry) pair separated by
   * comma.
   */
  case class FeatureListEntry(columnName: String, featureEntry: String)

}

