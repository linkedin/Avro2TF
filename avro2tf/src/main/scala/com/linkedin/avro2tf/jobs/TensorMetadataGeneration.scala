package com.linkedin.avro2tf.jobs

import java.nio.charset.StandardCharsets.UTF_8

import com.linkedin.avro2tf.configs.{DataType, Feature, TensorMetadata, TensorizeInTensorMetadata}
import scala.collection.mutable
import scala.io.Source

import com.linkedin.avro2tf.helpers.TensorizeInConfigHelper
import com.linkedin.avro2tf.parsers.TensorizeInParams
import com.linkedin.avro2tf.utils.Constants._
import com.linkedin.avro2tf.utils.{CommonUtils, Constants, IOUtils}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType}
import io.circe.generic.auto._
import io.circe.syntax._

/**
 * The Tensor Metadata job generates tensor metadata that will be later used in training with tensors.
 */
object TensorMetadataGeneration {

  /**
   * The main function to perform Tensor Metadata Generation job
   *
   * @param dataFrame Input data Spark DataFrame
   * @param params TensorizeIn parameters specified by user
   */
  def run(dataFrame: DataFrame, params: TensorizeInParams): Unit = {

    // NOTE: Intellij does not realise this import is used.
    import com.linkedin.avro2tf.configs.JsonCodecs._

    val fileSystem = FileSystem.get(dataFrame.sparkSession.sparkContext.hadoopConfiguration)
    val colsWithHashInfoCardinalityMapping = getColsWithHashInfoCardinalityMapping(params)
    val colsToFeatureCardinalityMapping: Map[String, Long] =
      getColsWithFeatureListCardinalityMapping(params, fileSystem) ++
        getColsOfIntOrLongCardinalityMapping(dataFrame, params) ++
        colsWithHashInfoCardinalityMapping

    val ntvColumns = dataFrame.columns.filter(
      colName => CommonUtils.isArrayOfNTV(dataFrame.schema(colName).dataType) ||

        // after hashing, may already be SparseVector or array of floats
        (colsWithHashInfoCardinalityMapping.contains(colName) &&
          (CommonUtils.isArrayOfFloat(dataFrame.schema(colName).dataType) ||
            CommonUtils.isSparseVector(dataFrame.schema(colName).dataType)))
    ).toSet

    var featuresTensorMetadata = generateTensorMetadata(
      params.tensorizeInConfig.features,
      colsToFeatureCardinalityMapping,
      ntvColumns)
    if (params.partitionFieldName.nonEmpty) {
      featuresTensorMetadata = featuresTensorMetadata :+ TensorMetadata(
        Constants.PARTITION_ID_FIELD_NAME,
        DataType.int,
        Seq(),
        Some(params.numPartitions)
      )
    }

    val labelsTensorMetadata = generateTensorMetadata(
      params.tensorizeInConfig.labels,
      colsToFeatureCardinalityMapping,
      ntvColumns)

    // Serialize TensorizeIn Tensor Metadata to JSON String
    val serializedTensorMetadata = TensorizeInTensorMetadata(featuresTensorMetadata, labelsTensorMetadata).asJson
      .toString()
    println(serializedTensorMetadata)
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

    // mapValues is lazy so use map to be safe for Spark
    TensorizeInConfigHelper.getColsHashInfo(params).map { case (col, hashInfo) =>
      col -> hashInfo.hashBucketSize.toLong
    }
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
    colsToFeatureCardinalityMapping: Map[String, Long],
    ntvColumns: Set[String]): Seq[TensorMetadata] = {

    featuresOrLabels.map {
      featureOrLabel =>
        val cardinality = colsToFeatureCardinalityMapping.get(featureOrLabel.outputTensorInfo.name)
        val shape = if (ntvColumns.contains(featureOrLabel.outputTensorInfo.name)) {
          cardinality.fold(featureOrLabel.outputTensorInfo.shape)(featureOrLabel.outputTensorInfo.shape :+ _.toInt)
        } else {
          featureOrLabel.outputTensorInfo.shape
        }

        TensorMetadata(
          name = featureOrLabel.outputTensorInfo.name,
          dtype = featureOrLabel.outputTensorInfo.dtype,
          shape = shape,
          cardinality = if (ntvColumns.contains(featureOrLabel.outputTensorInfo.name)) None else cardinality,
          isSparse = featureOrLabel.outputTensorInfo.isSparse)
    }
  }
}