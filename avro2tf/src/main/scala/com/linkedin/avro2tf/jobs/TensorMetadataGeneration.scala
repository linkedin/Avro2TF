package com.linkedin.avro2tf.jobs

import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.mutable
import scala.io.Source

import com.linkedin.avro2tf.configs.{Feature, TensorMetadata, TensorizeInTensorMetadata}
import com.linkedin.avro2tf.helpers.TensorizeInConfigHelper
import com.linkedin.avro2tf.parsers.TensorizeInParams
import com.linkedin.avro2tf.utils.Constants._
import com.linkedin.avro2tf.utils.{IOUtils, JsonUtil}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, LongType}

/**
 * The Tensor Metadata job generates tensor metadata that will be later used in training with tensors.
 *
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

    val featuresTensorMetadata = generateTensorMetadata(params.tensorizeInConfig.features, colsToFeatureCardinalityMapping)
    val labelsTensorMetadata = generateTensorMetadata(params.tensorizeInConfig.labels.getOrElse(Seq.empty), colsToFeatureCardinalityMapping)

    // Serialize TensorizeIn Tensor Metadata to JSON String
    val serializedTensorMetadata = JsonUtil.toJsonString(TensorizeInTensorMetadata(featuresTensorMetadata, labelsTensorMetadata))

    IOUtils.writeContentToHDFS(fileSystem, new Path(params.workingDir.tensorMetadataPath), serializedTensorMetadata, ENABLE_HDFS_OVERWRITE)
    fileSystem.close()
  }

  /**
   * Get the cardinality mapping of columns with feature list
   *
   * @param params TensorizeIn parameters specified by user
   * @param fileSystem A file system
   * @return A mapping of column name to its feature cardinality mapping
   */
  private def getColsWithFeatureListCardinalityMapping(params: TensorizeInParams, fileSystem: FileSystem): Map[String, Any] = {

    if (!params.workingDir.featureListPath.isEmpty) {
      // Get list statuses and block locations of the feature list files from the given path
      val featureListFiles = fileSystem.listFiles(new Path(params.workingDir.featureListPath), ENABLE_RECURSIVE)
      val colsWithFeatureListCardinalityMapping = new mutable.HashMap[String, Any]

      while (featureListFiles.hasNext) {
        // Get the source path of feature list file
        val sourcePath = featureListFiles.next().getPath
        // Get the column name of feature list
        val columnName = sourcePath.getName

        colsWithFeatureListCardinalityMapping
          .put(columnName, Source.fromInputStream(fileSystem.open(sourcePath), UTF_8.name()).getLines().size + 1)
      }

      colsWithFeatureListCardinalityMapping.toMap
    } else Map.empty
  }

  /**
   * Get the cardinality mapping of columns with hash information
   *
   * @param params TensorizeIn parameters specified by user
   * @return A mapping of column name to its cardinality
   */
  private def getColsWithHashInfoCardinalityMapping(params: TensorizeInParams): Map[String, Any] = {

    TensorizeInConfigHelper.concatFeaturesAndLabels(params)
      .filter(featureOrLabel => featureOrLabel.inputFeatureInfo.get.transformConfig match {
        case Some(transformConfig) if transformConfig.get(HASH_INFO).isDefined =>
          transformConfig(HASH_INFO).contains(HASH_INFO_HASH_BUCKET_SIZE)
        case _ => false
      })
      .map(feature => feature.outputTensorInfo.name -> feature.inputFeatureInfo.get
        .transformConfig
        .get(HASH_INFO)(HASH_INFO_HASH_BUCKET_SIZE))
      .toMap
  }

  /**
   * Get a mapping of column name of Integer or Long type to its cardinality
   *
   * @param dataFrame Input data Spark DataFrame
   * @param params TensorizeIn parameters specified by user
   * @return A mapping of column name to its cardinality
   */
  private def getColsOfIntOrLongCardinalityMapping(dataFrame: DataFrame, params: TensorizeInParams): Map[String, Long] = {

    val colsContainIntOrLongType = TensorizeInConfigHelper.concatFeaturesAndLabels(params)
      .map(featureOrLabel => featureOrLabel.outputTensorInfo.name)
      .filter(columnName => dataFrame.schema(columnName).dataType.isInstanceOf[IntegerType] ||
        dataFrame.schema(columnName).dataType.isInstanceOf[LongType])

    val maxRow = dataFrame
      .agg(colsContainIntOrLongType.map(_ -> MAX).toMap)
      .select(colsContainIntOrLongType.map(colName => col(s"$MAX($colName)").cast(LongType)) : _*)
      .head()

    colsContainIntOrLongType
      .map(colName => colName -> maxRow.getAs[Long](s"$MAX($colName)"))
      .toMap
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
    colsToFeatureCardinalityMapping: Map[String, Any]): Seq[TensorMetadata] = {

    featuresOrLabels
      .map(featureOrLabel => TensorMetadata(featureOrLabel.outputTensorInfo.name, featureOrLabel.outputTensorInfo.dtype,
        featureOrLabel.outputTensorInfo.shape.get, Some(colsToFeatureCardinalityMapping.get(featureOrLabel.outputTensorInfo.name))))
  }
}