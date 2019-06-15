package com.linkedin.avro2tf.helpers

import scala.collection.mutable

import com.linkedin.avro2tf.configs.{CombinerType, DataType, Feature, HashInfo}
import com.linkedin.avro2tf.parsers.TensorizeInParams
import com.linkedin.avro2tf.utils.Constants._

/**
 * Helper file for processing TensorizeIn Configuration
 *
 */
object TensorizeInConfigHelper {

  /**
   * Concatenate features and labels of TensorizeIn configuration
   *
   * @param params TensorizeIn parameters specified by user
   * @return A sequence of features
   */
  def concatFeaturesAndLabels(params: TensorizeInParams): Seq[Feature] = {

    params.tensorizeInConfig.labels.getOrElse(Seq.empty) ++ params.tensorizeInConfig.features
  }

  /**
   * Get columns with hash information specified in TensorizeIn configuration
   *
   * @param params TensorizeIn parameters specified by user
   * @return A sequence of column names
   */
  def getColsWithHashInfo(params: TensorizeInParams): Seq[String] = {

    concatFeaturesAndLabels(params)
      .filter(featureOrLabel => {
        featureOrLabel.inputFeatureInfo.get.transformConfig match {
          case Some(config) if config.contains(HASH_INFO) => true
          case _ => false
        }
      }).map(feature => feature.outputTensorInfo.name)
  }

  /**
   * Get the hashInfo of columns with hash information specified
   *
   * @param params TensorizeIn parameters specified by user
   * @return A sequence of column names
   */
  def getColsHashInfo(params: TensorizeInParams): Map[String, HashInfo] = {

    val colsHashInfo = new mutable.HashMap[String, HashInfo]

    concatFeaturesAndLabels(params).foreach {
      featureOrLabel => {
        featureOrLabel.inputFeatureInfo match {
          case Some(inputFeatureInfo) => inputFeatureInfo.transformConfig match {
            case Some(config) => config.get(HASH_INFO) match {
              case Some(hashInfo) =>
                if (!hashInfo.contains(HASH_INFO_HASH_BUCKET_SIZE)) {
                  throw new IllegalArgumentException(
                    s"Must specify $HASH_INFO_HASH_BUCKET_SIZE for the hashInfo of tensor ${
                      featureOrLabel.outputTensorInfo.name
                    }"
                  )
                }
                val hashBucketSize = hashInfo(HASH_INFO_HASH_BUCKET_SIZE).asInstanceOf[Int]
                val numHashFunctions = hashInfo.getOrElse(HASH_INFO_NUM_HASH_FUNCTIONS, 1).asInstanceOf[Int]
                val combinerType = hashInfo.getOrElse(HASH_INFO_COMBINER_TYPE, CombinerType.SUM.toString).toString
                colsHashInfo(featureOrLabel.outputTensorInfo.name) = HashInfo(
                  hashBucketSize,
                  numHashFunctions,
                  CombinerType.withName(combinerType))
              case _ =>
            }
            case _ =>
          }
          case _ =>
        }
      }
    }
    colsHashInfo.toMap
  }

  /**
   * Get output column names from TensorizeIn configuration
   *
   * @param params TensorizeIn parameters specified by user
   * @return A sequence of output column names
   */
  def getOutputTensorNames(params: TensorizeInParams): Seq[String] = {

    concatFeaturesAndLabels(params).map(feature => feature.outputTensorInfo.name)
  }

  /**
   * Get output tensor data types from TensorizeIn configuration
   *
   * @param params TensorizeIn parameters specified by user
   * @return A sequence of output tensor data types
   */
  def getOutputTensorDataTypes(params: TensorizeInParams): Map[String, DataType.Value] = {

    concatFeaturesAndLabels(params).map(feature => feature.outputTensorInfo.name -> feature.outputTensorInfo.dataType)
      .toMap
  }
}