package com.linkedin.avro2tf.helpers

import scala.collection.mutable

import com.databricks.spark.avro._
import com.linkedin.avro2tf.configs.{DataType, Feature}
import com.linkedin.avro2tf.parsers.TensorizeInParams
import com.linkedin.avro2tf.utils.Constants._
import com.linkedin.avro2tf.utils.{CommonUtils, Constants, IOUtils}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{expr, udf}
import org.apache.spark.sql._

/**
 * Helper file for TensorizeIn job
 *
 */
object TensorizeInJobHelper {
  /**
   * Read data from HDFS to Spark DataFrame
   *
   * @param session SparkSession
   * @param params TensorizeIn parameters specified by user
   * @return Spark DataFrame
   */
  def readDataFromHDFS(session: SparkSession, params: TensorizeInParams): DataFrame = {

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
   * @param params TensorizeIn parameters specified by user
   */
  def saveDataToHDFS(dataFrame: DataFrame, params: TensorizeInParams): Unit = {

    val outputPath = params.executionMode match {
      case TRAINING_EXECUTION_MODE => params.workingDir.trainingDataPath
      case VALIDATION_EXECUTION_MODE => params.workingDir.validationDataPath
      case TEST_EXECUTION_MODE => params.workingDir.testDataPath
    }

    // Get a Spark DataFrame based on whether user specifies the number of output files; if not specify, the number is set to negative
    val dataFrameRepartitioned =
      if (params.numOfOutputFiles < 0) {
        if (params.enableShuffle) dataFrame.repartition() else dataFrame
      } else {
        if (params.enableShuffle || dataFrame.rdd.getNumPartitions < params.numOfOutputFiles) {
          dataFrame.repartition(params.numOfOutputFiles)
        } else {
          dataFrame.coalesce(params.numOfOutputFiles)
        }
      }

    // Write data to HDFS
    if (params.outputFormat.equals(TF_RECORD)) {
      val dataFramePrepared = prepareTFRecord(dataFrameRepartitioned, params)
      // SequenceExample record type supports array of array data type to be saved as TFRecord
      dataFramePrepared.write.mode("overwrite").format("tfrecords").option("recordType", "SequenceExample")
        .save(outputPath)
    } else {
      dataFrameRepartitioned.write.mode(SaveMode.Overwrite).avro(outputPath)
    }
  }

  /**
   * Sanity check on tensor names specified in TensorizeIn config
   *
   * @param dataFrame Spark DataFrame
   * @param params TensorizeIn parameters specified by user
   */
  def tensorsNameCheck(dataFrame: DataFrame, params: TensorizeInParams): Unit = {

    // Get all existing input data frame column names
    val allExistingColumnNames = dataFrame.columns.toSet

    // Combine the feature and label tensor lists
    val tensors = TensorizeInConfigHelper.concatFeaturesAndLabels(params)

    // Build an empty set of output tensors to check duplication
    val outputTensors = new mutable.HashSet[String]

    tensors.foreach {
      tensor => {
        // Get tensor name from output tensor info
        val tensorName = tensor.outputTensorInfo.name

        // Check if tensor name is used more than once in TensorizeIn config
        if (!outputTensors.contains(tensorName)) {
          outputTensors.add(tensorName)
        } else {
          throw new IllegalArgumentException(s"Output tensor name: $tensorName is used more than once in your config.")
        }

        // Check if tensor name already exists in the list of original data frame column names
        if (checkNewColNameOverlapWithExistingCols(tensor, allExistingColumnNames)) {
          throw new IllegalArgumentException(s"Output tensor name: $tensorName already exist in the original data frame column name.")
        }
      }
    }
    params.extraColumnsToKeep.foreach {
      columnName => if(columnName.contains(Constants.COLUMN_NAME_ALIAS_DELIMITER)){
        val exprAndAlias = columnName.trim.split(Constants.COLUMN_NAME_ALIAS_DELIMITER)
        require(exprAndAlias.length == 2, s"Invalid column name specified: $columnName in --extra-columns-to-keep")
        require(!outputTensors.contains(exprAndAlias.last), s"Column alias: ${exprAndAlias.last} already used by output tensors")
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
   * @param params TensorizeIn parameters specified by user
   * @return A Spark DataFrame
   */
  private def prepareTFRecord(dataFrame: DataFrame, params: TensorizeInParams): DataFrame = {

    val outputTensorDataTypes = TensorizeInConfigHelper.getOutputTensorDataTypes(params)
    val newConvertedColumns = new mutable.ArrayBuffer[Column]
    val convertedColumnNames = new mutable.HashSet[String]
    val dataFrameSchema = dataFrame.schema

    outputTensorDataTypes.foreach {
      case (columnName, dataType) =>
        if (dataType == DataType.sparseVector) {
          // Construct two separate indices and values columns for SparseVector data type
          newConvertedColumns.append(expr(s"$columnName.$INDICES").alias(s"$columnName-$INDICES"))
          newConvertedColumns.append(expr(s"$columnName.$VALUES").alias(s"$columnName-$VALUES"))
          convertedColumnNames.add(columnName)
        } else if (CommonUtils.isArrayOfString(dataFrameSchema(columnName).dataType)) {
          // Construct a new array of String array column for String array data type to be used with SequenceExample
          newConvertedColumns.append(wrapStringArray(dataFrame(columnName)).name(columnName))
          convertedColumnNames.add(columnName)
        }
    }

    val nonConvertedColumns = dataFrame.columns.filter(colName => !convertedColumnNames.contains(colName))
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
      stringSeq: Seq[String] => Seq(stringSeq)
    }
  }
}