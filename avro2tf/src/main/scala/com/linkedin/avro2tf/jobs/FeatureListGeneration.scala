package com.linkedin.avro2tf.jobs

import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.mutable

import com.linkedin.avro2tf.configs.DataType
import com.linkedin.avro2tf.helpers.TensorizeInConfigHelper
import com.linkedin.avro2tf.parsers.TensorizeInParams
import com.linkedin.avro2tf.utils.CommonUtils
import com.linkedin.avro2tf.utils.Constants._
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, Row}

/**
 * The Feature List Generation job generates feature list that will be later used in training with tensors.
 *
 */
class FeatureListGeneration {

  /**
   * The main function to perform Feature List Generation job
   *
   * @param dataFrame Input data Spark DataFrame
   * @param params TensorizeIn parameters specified by user
   */
  def run(dataFrame: DataFrame, params: TensorizeInParams): Unit = {

    val fileSystem = FileSystem.get(dataFrame.sparkSession.sparkContext.hadoopConfiguration)

    val featureListPath = new Path(params.workingDir.featureListPath)
    // Make sure we have an empty feature list dir before generating new ones
    fileSystem.delete(featureListPath, ENABLE_RECURSIVE)
    fileSystem.mkdirs(featureListPath)

    // Only collect those without external feature list and hash information specified in TensorizeIn configuration
    val colsToCollectFeatureList = params.tensorizeInConfig.features.map(feature => feature.outputTensorInfo.name) diff
      (processExternalFeatureList(params, fileSystem) ++ TensorizeInConfigHelper.getColsWithHashInfo(params))

    collectAndSaveFeatureList(dataFrame, params, fileSystem, colsToCollectFeatureList)
    writeFeatureList(params, fileSystem)

    fileSystem.close()
  }

  /**
   * Process external feature lists by copying to working directory and collecting their column names
   *
   * @param params TensorizeIn parameters specified by user
   * @param fileSystem A file system
   * @return A sequence of column names
   */
  private def processExternalFeatureList(params: TensorizeInParams, fileSystem: FileSystem): Seq[String] = {

    if (!params.externalFeaturesListPath.isEmpty) {
      val colsWithExternalFeatureList = new mutable.HashSet[String]
      val colsWithHashInfo = TensorizeInConfigHelper.getColsWithHashInfo(params)

      // Get list statuses and block locations of the external feature list files from the given path
      val externalFeatureListFiles = fileSystem.listFiles(new Path(params.externalFeaturesListPath), ENABLE_RECURSIVE)
      val destinationPath = params.workingDir.featureListPath

      while (externalFeatureListFiles.hasNext) {
        // Get the source path of external feature list file
        val sourcePath = externalFeatureListFiles.next().getPath
        // Get the column name of external feature list
        // (Note: User is required to use the corresponding column name as their external feature list file name)
        val columnName = sourcePath.getName

        // In case user does not specify a right external feature list which they want to use
        if (!params.tensorizeInConfig.features.map(feature => feature.outputTensorInfo.name).contains(columnName)) {
          throw new IllegalArgumentException(s"External feature list $columnName does not exist in user specified TensorizeIn output tensor names.")
        }

        // Exclude external feature list of columns with hash information
        if (!colsWithHashInfo.contains(columnName)) {
          colsWithExternalFeatureList.add(columnName)
          // Move external feature list path to destination path with its column name as file name
          FileUtil.copy(
            fileSystem, sourcePath, fileSystem, new Path(s"$destinationPath/$columnName"),
            DISABLE_DELETE_SOURCE, ENABLE_HDFS_OVERWRITE, fileSystem.getConf)
        }
      }

      colsWithExternalFeatureList.toSeq
    } else {
      Seq.empty
    }
  }

  /**
   * Collect and save feature list
   *
   * @param dataFrame Input data Spark DataFrame
   * @param params TensorizeIn parameters specified by user
   * @param fileSystem A file system
   * @param colsToCollectFeatureList A sequence of columns to collect feature lists
   */
  private def collectAndSaveFeatureList(
    dataFrame: DataFrame,
    params: TensorizeInParams,
    fileSystem: FileSystem,
    colsToCollectFeatureList: Seq[String]): Unit = {

    import dataFrame.sparkSession.implicits._
    val dataFrameSchema = dataFrame.schema
    val tmpFeatureListPath = s"${params.workingDir.rootPath}/$TMP_FEATURE_LIST"
    fileSystem.delete(new Path(tmpFeatureListPath), ENABLE_RECURSIVE)
    val outputTensorDataTypes = TensorizeInConfigHelper.getOutputTensorDataTypes(params)

    dataFrame.flatMap {
      row => {
        colsToCollectFeatureList.flatMap {
          colName => {
            if (CommonUtils.isArrayOfNTV(dataFrameSchema(colName).dataType)) {
              val nameTermValueRows = row.getAs[Seq[Row]](colName)
              if (nameTermValueRows != null) {
                nameTermValueRows.map(
                  nameTermValueRow =>
                    TensorizeIn.FeatureListEntry(
                      colName,
                      s"${nameTermValueRow.getAs[String](NTV_NAME)},${nameTermValueRow.getAs[String](NTV_TERM)}"))
              } else {
                Seq.empty
              }
            } else if (CommonUtils.isArrayOfString(dataFrameSchema(colName).dataType) &&
              (outputTensorDataTypes(colName) == DataType.int || outputTensorDataTypes(colName) == DataType.long)) {
              val strings = row.getAs[Seq[String]](colName)
              if (strings != null) strings.map(string => TensorizeIn.FeatureListEntry(colName, string)) else Seq.empty
            }
            else {
              Seq.empty
            }
          }
        }
      }
    }
      .distinct
      .sort(COLUMN_NAME)
      .map(featureListEntry => s"${featureListEntry.columnName},${featureListEntry.featureEntry}")
      .rdd
      .saveAsTextFile(tmpFeatureListPath)
  }

  /**
   * Write feature list as text file to HDFS
   *
   * @param params TensorizeIn parameters specified by user
   * @param fileSystem A file system
   */
  private def writeFeatureList(params: TensorizeInParams, fileSystem: FileSystem): Unit = {

    // Write feature lists to HDFS
    val tmpFeatureListPath = s"${params.workingDir.rootPath}/$TMP_FEATURE_LIST"
    var currentColumnName: String = EMPTY_STRING
    var writer: OutputStreamWriter = null
    fileSystem.globStatus(new Path(s"$tmpFeatureListPath/$FILE_NAME_REGEX"))
      .map(fileStatus => fileStatus.getPath.toString)
      .toSeq
      .sortWith((path1, path2) => path1 < path2)
      .foreach(
        fileName => {
          val inputStream = fileSystem.open(new Path(fileName))

          scala.io.Source.fromInputStream(inputStream, UTF_8.name())
            .getLines()
            .foreach(
              line => {
                // Get column name of FeatureListEntry
                val columnName = line.split(SPLIT_REGEX).head

                // Reassign the current column name variable if column name is different
                if (columnName != currentColumnName) {
                  currentColumnName = columnName
                  val outputStream = fileSystem.create(
                    new Path(s"${params.workingDir.featureListPath}/$columnName")
                  )

                  if (writer != null) writer.close()
                  writer = new OutputStreamWriter(outputStream, UTF_8.name())
                }

                // Get the feature entry of FeatureListEntry
                val featureListEntry = line.stripPrefix(columnName + SPLIT_REGEX)
                writer.write(s"$featureListEntry\n")
              })
          inputStream.close()
        })

    if (writer != null) writer.close()
    fileSystem.delete(new Path(tmpFeatureListPath), ENABLE_RECURSIVE)
  }
}