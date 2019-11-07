package com.linkedin.avro2tf.helpers

import scala.collection.mutable

import com.databricks.spark.avro._
import com.linkedin.avro2tf.configs.{DataType, Feature}
import com.linkedin.avro2tf.constants.Constants
import com.linkedin.avro2tf.parsers.Avro2TFParams
import com.linkedin.avro2tf.constants.Constants._
import com.linkedin.avro2tf.utils.{CommonUtils, IOUtils, TrainingMode}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{expr, udf}
import org.apache.spark.sql._

/**
 * Helper file for Avro2TF job
 *
 */
object Avro2TFJobHelper {
  /**
   * Read data from HDFS to Spark DataFrame
   *
   * @param session SparkSession
   * @param params Avro2TF parameters specified by user
   * @return Spark DataFrame
   */
  def readDataFromHDFS(session: SparkSession, params: Avro2TFParams): DataFrame = {

    val inputData = if (params.inputDateRange.nonEmpty) {
      // Read Avro data based on input date range
      IOUtils.readAvro(session, params.inputPaths.head, params.inputDateRange.head, params.inputDateRange.last)
    } else if (params.inputDaysRange.nonEmpty) {
      // Read Avro data based on input days range
      IOUtils.readAvro(session, params.inputPaths.head, params.inputDaysRange.head, params.inputDaysRange.last)
    } else {
      // Read Avro given one or multiple input paths
      IOUtils.readAvro(session, params.inputPaths: _*)
    }

    // Check if repartition is needed
    if (inputData.rdd.getNumPartitions < params.minParts) inputData.repartition(params.minParts) else inputData
  }

  /**
   * Save Spark DataFrame data to HDFS
   *
   * @param dataFrame Spark DataFrame
   * @param params Avro2TF parameters specified by user
   */
  def saveDataToHDFS(dataFrame: DataFrame, params: Avro2TFParams): Unit = {

    val outputPath = params.executionMode match {
      case TrainingMode.training => params.workingDir.trainingDataPath
      case TrainingMode.validation => params.workingDir.validationDataPath
      case TrainingMode.test => params.workingDir.testDataPath
    }

    // Get a Spark DataFrame based on whether user specifies the number of output files; if not specify, the number is set to negative
    var dataFrameRepartitioned = repartitionData(dataFrame, params.numOfOutputFiles, params.enableShuffle)

    if (params.outputFormat.equals(TF_RECORD)) {
      dataFrameRepartitioned = prepareTFRecord(dataFrameRepartitioned, params)
    }

    val partitionOutput = params.partitionFieldName.nonEmpty &&
      dataFrameRepartitioned.columns.contains(params.partitionFieldName)
    val dataFrameWriter = if (partitionOutput) {
      dataFrameRepartitioned = dataFrameRepartitioned.withColumn(
        Constants.PARTITION_COLUMN_NAME,
        dataFrameRepartitioned(Constants.PARTITION_ID_FIELD_NAME))
      dataFrameRepartitioned.write.partitionBy(Constants.PARTITION_COLUMN_NAME).mode(SaveMode.Overwrite)
    } else {
      dataFrameRepartitioned.write.mode(SaveMode.Overwrite)
    }

    // Write data to HDFS
    if (params.outputFormat.equals(TF_RECORD)) {

      // SequenceExample record type supports array of array data type to be saved as TFRecord
      dataFrameWriter.format("tfrecords").option("recordType", "SequenceExample").save(outputPath)
    } else {
      dataFrameWriter.avro(outputPath)
    }
  }

  /**
   * Repartition the data according to num of output files and shuffle option
   *
   * @param dataFrame The data frame
   * @param numOfOutputFiles The number of output files
   * @param enableShuffle The shuffle option
   * @return The repartitioned data frame
   */
  def repartitionData(dataFrame: DataFrame, numOfOutputFiles: Int, enableShuffle: Boolean): DataFrame = {

    if (numOfOutputFiles < 0) {
      if (enableShuffle) {
        dataFrame.repartition()
      } else {
        dataFrame
      }
    } else if (enableShuffle || dataFrame.rdd.getNumPartitions < numOfOutputFiles) {
      dataFrame.repartition(numOfOutputFiles)
    } else {
      dataFrame.coalesce(numOfOutputFiles)
    }
  }

  /**
   * Sanity check on tensor names specified in Avro2TF config
   *
   * @param dataFrame Spark DataFrame
   * @param params Avro2TF parameters specified by user
   */
  def tensorsNameCheck(dataFrame: DataFrame, params: Avro2TFParams): Unit = {

    // Get all existing input data frame column names
    val allExistingColumnNames = dataFrame.columns.toSet

    // Combine the feature and label tensor lists
    val tensors = Avro2TFConfigHelper.concatFeaturesAndLabels(params)

    // Build an empty set of output tensors to check duplication
    val outputTensors = new mutable.HashSet[String]

    tensors.foreach {
      tensor => {
        // Get tensor name from output tensor info
        val tensorName = tensor.outputTensorInfo.name

        // Check if tensor name is used more than once in Avro2TF config
        if (!outputTensors.add(tensorName)) {
          throw new IllegalArgumentException(s"Output tensor name: $tensorName is used more than once in your config.")
        }

        // Check if tensor name already exists in the list of original data frame column names
        if (checkNewColNameOverlapWithExistingCols(tensor, allExistingColumnNames)) {
          throw new IllegalArgumentException(s"Output tensor name: $tensorName already exist in the original data frame column name.")
        }
      }
    }
    params.extraColumnsToKeep.foreach {
      columnName =>
        if (columnName.contains(Constants.COLUMN_NAME_ALIAS_DELIMITER)) {
          val exprAndAlias = columnName.trim.split(Constants.COLUMN_NAME_ALIAS_DELIMITER)
          require(exprAndAlias.length == 2, s"Invalid column name specified: $columnName in --extra-columns-to-keep")
          require(
            !outputTensors.contains(exprAndAlias.last),
            s"Column alias: ${exprAndAlias.last} already used by output tensors")
        }
    }
    // make sure that all tensors defined in the feature sharing list are valid output tensors
    if (!params.tensorsSharingFeatureLists.isEmpty) {
      val tensorsGroups = params.tensorsSharingFeatureLists
      val tensorsInGroups = tensorsGroups.flatten
      val outputTensorNames = tensors.map(tensor => tensor.outputTensorInfo.name)
      if (!tensorsInGroups.forall(tensor => outputTensorNames.contains(tensor))) {
        throw new IllegalArgumentException(
          s"Invalid output tensor name in --tensors-sharing-feature-lists: " +
            s"$tensorsGroups. Valid names are $outputTensorNames.")
      }
    }
  }

  /**
   * Check if the name of a output tensor overlap with existing columns names.
   * if the tensor name is the same as columnExpr, it is not counted as overlapping.
   *
   * @param tensor Infos about a tensor specified by user.
   * @param existingColumnNames All the column names in input data.
   * @return
   */
  private def checkNewColNameOverlapWithExistingCols(tensor: Feature, existingColumnNames: Set[String]): Boolean = {

    val tensorName = tensor.outputTensorInfo.name
    tensor.inputFeatureInfo match {
      case Some(inputFeatureInfo) => inputFeatureInfo.columnExpr match {
        case Some(columnExpr) =>
          if (columnExpr != tensorName) {
            existingColumnNames.contains(tensorName)
          }
          else {
            false
          }
        case None => existingColumnNames.contains(tensorName)
      }
      case None => false
    }
  }

  /**
   * Prepare data to be saved as TFRecord
   *
   * @param dataFrame Input data Spark DataFrame
   * @param params Avro2TF parameters specified by user
   * @return A Spark DataFrame
   */
  private def prepareTFRecord(dataFrame: DataFrame, params: Avro2TFParams): DataFrame = {

    val newConvertedColumns = new mutable.ArrayBuffer[Column]
    val convertedColumnNames = new mutable.HashSet[String]
    val invalidColumns = new mutable.HashSet[String]
    val dataFrameSchema = dataFrame.schema

    dataFrame.columns.foreach {
      columnName =>
        if (CommonUtils.isSparseVector(dataFrameSchema(columnName).dataType)) {
          // Construct two separate indices and values columns for SparseVector data type
          newConvertedColumns.append(expr(s"$columnName.$INDICES").alias(s"${columnName}_$INDICES"))
          newConvertedColumns.append(expr(s"$columnName.$VALUES").alias(s"${columnName}_$VALUES"))
          convertedColumnNames.add(columnName)
        } else if (CommonUtils.isArrayOfString(dataFrameSchema(columnName).dataType)) {
          // Construct a new array of String array column for String array data type to be used with SequenceExample
          newConvertedColumns.append(wrapStringArray(dataFrame(columnName)).name(columnName))
          convertedColumnNames.add(columnName)
        } else if (!CommonUtils.isValidTFRecordType(dataFrameSchema(columnName).dataType)) {
          invalidColumns.add(columnName)
        }
    }

    val nonConvertedColumns = dataFrame.columns
      .filter(colName => !convertedColumnNames.contains(colName) && !invalidColumns.contains(colName))
      .map(dataFrame(_))
    dataFrame.select(nonConvertedColumns ++ newConvertedColumns: _*)
  }

  /**
   * Spark UDF function to wrap String array with an array
   *
   * @return A Spark UserDefinedFunction
   */
  private def wrapStringArray: UserDefinedFunction = {

    udf {
      stringSeq: Seq[String] => Seq(stringSeq.map(x => Seq(x)))
    }
  }
}