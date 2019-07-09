package com.linkedin.avro2tf.jobs

import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.mutable
import scala.io.Source

import com.linkedin.avro2tf.configs.{DataType, Feature, TensorMetadata, TensorizeInTensorMetadata}
import com.linkedin.avro2tf.helpers.TensorizeInConfigHelper
import com.linkedin.avro2tf.parsers.TensorizeInParams
import com.linkedin.avro2tf.utils.Constants._
import com.linkedin.avro2tf.utils.{Constants, IOUtils, JsonUtil}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType}

/**
 * The Tensor Metadata job generates tensor metadata that will be later used in training with tensors.
 */
class TensorMetadataGeneration {

  /**
   * The main function to perform Tensor Metadata Generation job
   *
   * @param dataFrame Input data Spark DataFrame
   * @param params TensorizeIn parameters specified by user
   */
  def run(dataFrame: DataFrame, params: TensorizeInParams): Unit = {

    val fileSystem = FileSystem.get(dataFrame.sparkSession.sparkContext.hadoopConfiguration)
    val colsToFeatureCardinalityMapping = getColsWithFeatureListCardinalityMapping(params, fileSystem) ++
      getColsOfIntOrLongCardinalityMapping(dataFrame, params) ++
      getColsWithHashInfoCardinalityMapping(params)

    var featuresTensorMetadata = generateTensorMetadata(
      params.tensorizeInConfig.features,
      colsToFeatureCardinalityMapping)
    if (params.partitionFieldName.nonEmpty) {
      featuresTensorMetadata = featuresTensorMetadata :+ TensorMetadata(
        Constants.PARTITION_ID_FIELD_NAME,
        DataType.int.toString,
        Seq(),
        Some(params.numPartitions)
      )
    }

    val labelsTensorMetadata = generateTensorMetadata(
      params.tensorizeInConfig.labels.getOrElse(Seq.empty),
      colsToFeatureCardinalityMapping)

    // Serialize TensorizeIn Tensor Metadata to JSON String
    val serializedTensorMetadata = JsonUtil
      .toJsonString(TensorizeInTensorMetadata(featuresTensorMetadata, labelsTensorMetadata))

    IOUtils
      .writeContentToHDFS(
        fileSystem,
        new Path(params.workingDir.tensorMetadataPath),
        serializedTensorMetadata,
        ENABLE_HDFS_OVERWRITE)
    fileSystem.close()
  }

  /**
   * Get the cardinality mapping of columns with feature list
   *
   * @param params TensorizeIn parameters specified by user
   * @param fileSystem A file system
   * @return A mapping of column name to its feature cardinality mapping
   */
  private def getColsWithFeatureListCardinalityMapping(
    params: TensorizeInParams,
    fileSystem: FileSystem): Map[String, Long] = {

    if (!params.workingDir.featureListPath.isEmpty) {
      // Get list statuses and block locations of the feature list files from the given path
      val featureListFiles = fileSystem.listFiles(new Path(params.workingDir.featureListPath), ENABLE_RECURSIVE)
      val colsWithFeatureListCardinalityMapping = new mutable.HashMap[String, Long]

      while (featureListFiles.hasNext) {
        // Get the source path of feature list file
        val sourcePath = featureListFiles.next().getPath
        // Get the column name of feature list
        val columnName = sourcePath.getName

        colsWithFeatureListCardinalityMapping
          .put(columnName, Source.fromInputStream(fileSystem.open(sourcePath), UTF_8.name()).getLines().size + 1)
      }

      colsWithFeatureListCardinalityMapping.toMap
    } else {
      Map.empty
    }
  }

  /**
   * Get the cardinality mapping of columns with hash information
   *
   * @param params TensorizeIn parameters specified by user
   * @return A mapping of column name to its cardinality
   */
  private def getColsWithHashInfoCardinalityMapping(params: TensorizeInParams): Map[String, Long] = {

    TensorizeInConfigHelper.getColsHashInfo(params).map(x => x._1 -> x._2.hashBucketSize.toLong)
  }

  /**
   * Get a mapping of column name of Integer or Long type to its cardinality
   *
   * @param dataFrame Input data Spark DataFrame
   * @param params TensorizeIn parameters specified by user
   * @return A mapping of column name to its cardinality
   */
  private def getColsOfIntOrLongCardinalityMapping(
    dataFrame: DataFrame,
    params: TensorizeInParams): Map[String, Long] = {

    val intOrLongColNames = TensorizeInConfigHelper.concatFeaturesAndLabels(params)
      .map(featureOrLabel => featureOrLabel.outputTensorInfo.name)
      .filter(
        columnName => dataFrame.schema(columnName).dataType.isInstanceOf[IntegerType] ||
          dataFrame.schema(columnName).dataType.isInstanceOf[LongType])

    if (intOrLongColNames.isEmpty) {
      Map.empty
    } else {
      val intOrLongCols = intOrLongColNames
        .map(columnName => max(col(columnName)))

      val maxRow = dataFrame
        // N.B. For improved performance, we use the .agg() overload that takes Columns instead of String expressions.
        .agg(intOrLongCols.head, intOrLongCols.tail: _*)
        .head

      intOrLongColNames
        .map(colName => colName -> maxRow.getAs[Number](s"$MAX($colName)").longValue())
        .toMap
    }
  }

  /**
   * The main function to generate Tensor Metadata
   *
   * @param featuresOrLabels A sequence of features or labels
   * @param colsToFeatureCardinalityMapping A mapping of column name to its cardinality
   * @return A sequence of Tensor metadata
   */
  private def generateTensorMetadata(
    featuresOrLabels: Seq[Feature],
    colsToFeatureCardinalityMapping: Map[String, Long]): Seq[TensorMetadata] = {

    featuresOrLabels.map {
      featureOrLabel =>
        if (featureOrLabel.outputTensorInfo.dataType == DataType.sparseVector) {
          val shape = colsToFeatureCardinalityMapping.get(featureOrLabel.outputTensorInfo.name) match {
            case Some(cardinality) => featureOrLabel.outputTensorInfo.shape.get :+ cardinality.toInt
            case None => featureOrLabel.outputTensorInfo.shape.get
          }
          TensorMetadata(
            featureOrLabel.outputTensorInfo.name,
            DataType.float.toString,
            shape,
            None,
            isSparse = true
          )
        } else {
          TensorMetadata(
            featureOrLabel.outputTensorInfo.name,
            featureOrLabel.outputTensorInfo.dtype,
            featureOrLabel.outputTensorInfo.shape.get,
            colsToFeatureCardinalityMapping.get(featureOrLabel.outputTensorInfo.name))
        }
    }
  }
}