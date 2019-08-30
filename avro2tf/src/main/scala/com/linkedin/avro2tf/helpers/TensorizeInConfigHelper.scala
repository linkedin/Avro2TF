package com.linkedin.avro2tf.helpers

import com.linkedin.avro2tf.configs.{DataType, Feature, HashInfo}
import com.linkedin.avro2tf.parsers.TensorizeInParams

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

    params.tensorizeInConfig.labels ++ params.tensorizeInConfig.features
  }

  /**
   * Get columns with hash information specified in TensorizeIn configuration
   *
   * @param params TensorizeIn parameters specified by user
   * @return A sequence of column names
   */
  def getColsWithHashInfo(params: TensorizeInParams): Seq[String] = {

    getColsHashInfo(params).keys.toSeq
  }

  /**
   * Get the hashInfo of columns with hash information specified
   *
   * @param params TensorizeIn parameters specified by user
   * @return A sequence of column names
   */
  def getColsHashInfo(params: TensorizeInParams): Map[String, HashInfo] = {

    concatFeaturesAndLabels(params).flatMap { feature =>
      for {
        inputFeatureInfo <- feature.inputFeatureInfo
        transformConfig <- inputFeatureInfo.transformConfig
        hashInfo <- transformConfig.hashInfo
      } yield feature.outputTensorInfo.name -> hashInfo
    }.toMap
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

    concatFeaturesAndLabels(params).map(feature => feature.outputTensorInfo.name -> feature.outputTensorInfo.dtype)
      .toMap
  }

  /**
   * Get output tensor sparsity from TensorizeIn configuration
   *
   * @param params TensorizeIn parameters specified by user
   * @return A map of output tensor name to isSparse
   */
  def getOutputTensorSparsity(params: TensorizeInParams): Map[String, Boolean] = {

    concatFeaturesAndLabels(params).map(feature => feature.outputTensorInfo.name -> feature.outputTensorInfo.isSparse)
      .toMap
  }
}