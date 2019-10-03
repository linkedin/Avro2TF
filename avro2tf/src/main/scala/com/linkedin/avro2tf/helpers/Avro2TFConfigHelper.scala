package com.linkedin.avro2tf.helpers

import com.linkedin.avro2tf.configs.{DataType, Feature, HashInfo}
import com.linkedin.avro2tf.parsers.Avro2TFParams

/**
 * Helper file for processing Avro2TF Configuration
 *
 */
object Avro2TFConfigHelper {

  /**
   * Concatenate features and labels of Avro2TF configuration
   *
   * @param params Avro2TF parameters specified by user
   * @return A sequence of features
   */
  def concatFeaturesAndLabels(params: Avro2TFParams): Seq[Feature] = {

    params.avro2TFConfig.labels ++ params.avro2TFConfig.features
  }

  /**
   * Get columns with hash information specified in Avro2TF configuration
   *
   * @param params Avro2TF parameters specified by user
   * @return A sequence of column names
   */
  def getColsWithHashInfo(params: Avro2TFParams): Seq[String] = {

    getColsHashInfo(params).keys.toSeq
  }

  /**
   * Get the hashInfo of columns with hash information specified
   *
   * @param params Avro2TF parameters specified by user
   * @return A map of column name to hashInfo
   */
  def getColsHashInfo(params: Avro2TFParams): Map[String, HashInfo] = {

    concatFeaturesAndLabels(params).flatMap { feature =>
      for {
        inputFeatureInfo <- feature.inputFeatureInfo
        transformConfig <- inputFeatureInfo.transformConfig
        hashInfo <- transformConfig.hashInfo
      } yield feature.outputTensorInfo.name -> hashInfo
    }.toMap
  }

  /**
   * Get output column names from Avro2TF configuration
   *
   * @param params Avro2TF parameters specified by user
   * @return A sequence of output column names
   */
  def getOutputTensorNames(params: Avro2TFParams): Seq[String] = {

    concatFeaturesAndLabels(params).map(feature => feature.outputTensorInfo.name)
  }

  /**
   * Get output tensor data types from Avro2TF configuration
   *
   * @param params Avro2TF parameters specified by user
   * @return A map of output tensor name to its data type
   */
  def getOutputTensorDataTypes(params: Avro2TFParams): Map[String, DataType.Value] = {

    concatFeaturesAndLabels(params).map(feature => feature.outputTensorInfo.name -> feature.outputTensorInfo.dtype)
      .toMap
  }

  /**
   * Get output tensor sparsity from Avro2TF configuration
   *
   * @param params Avro2TF parameters specified by user
   * @return A map of output tensor name to isSparse
   */
  def getOutputTensorSparsity(params: Avro2TFParams): Map[String, Boolean] = {

    concatFeaturesAndLabels(params).map(feature => feature.outputTensorInfo.name -> feature.outputTensorInfo.isSparse)
      .toMap
  }
}