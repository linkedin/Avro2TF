package com.linkedin.avro2tf.parsers

import com.linkedin.avro2tf.configs.{Feature, InputFeatureInfo, OutputTensorInfo, TensorizeInConfiguration}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

/**
 * Parser file for TensorizeIn configuration
 *
 */
object TensorizeInConfigParser {

  /**
   * Get the TensorizeIn configuration with sanity check
   *
   * @param jsonString JSON format of TensorizeIn configuration
   * @return TensorizeIn configuration
   */
  def getTensorizeInConfiguration(jsonString: String): TensorizeInConfiguration = {

    // Define implicit JSON4S default format
    implicit val formats: DefaultFormats.type = DefaultFormats

    // Use JSON4S to parse and extract TensorizeIn configuration
    val config = parse(jsonString).extract[TensorizeInConfiguration]

    TensorizeInConfiguration(
      features = sanityCheck(config.features),
      labels = Some(sanityCheck(config.labels.getOrElse(Seq.empty)))
    )
  }

  /**
   * Sanity check on features or labels in TensorizeIn configuration
   *
   * @param featuresOrLabels A sequence of features or labels to be checked
   * @return A sequence of features or labels
   */
  private def sanityCheck(featuresOrLabels: Seq[Feature]): Seq[Feature] = {

    val checkedItems = featuresOrLabels.map(featureOrLabel => checkColumnExprAndConfig(featureOrLabel))

    checkedItems.map(checkedItem => checkShape(checkedItem))
  }

  /**
   * Check column expression and column configuration in input feature information.
   * Add default column expression value with the name in output tensor information if input feature information does not exist
   *
   * @param feature Feature in TensorizeIn configuration
   * @return Feature in TensorizeIn configuration
   */
  private def checkColumnExprAndConfig(feature: Feature): Feature = {

    feature.copy(inputFeatureInfo = feature.inputFeatureInfo match {
      case Some(inputFeatureInfo) => inputFeatureInfo.columnExpr match {
        case Some(columnExpr) => checkIfColumnExprAndConfigBothExist(inputFeatureInfo, columnExpr)
        case None => checkIfColumnExprAndConfigBothNotExist(inputFeatureInfo, feature.outputTensorInfo)
      }
      case None => Some(InputFeatureInfo(columnExpr = Some(feature.outputTensorInfo.name), None, None))
    })
  }

  /**
   * Check if column expression and column configuration both exist in input feature information.
   * Throw an exception when column expression and column configuration both exist in input feature information
   *
   * @param inputFeatureInfo Input feature information
   * @param columnExpr Column expression in input feature information
   * @return Optional input feature information
   */
  private def checkIfColumnExprAndConfigBothExist(inputFeatureInfo: InputFeatureInfo, columnExpr: String): Option[InputFeatureInfo] = {

    inputFeatureInfo.columnConfig match {
      case Some(columnConfig) => throw new IllegalArgumentException(s"Column expression $columnExpr and column configuration" +
        s" $columnConfig should not exist at the same time.")
      case None => Some(inputFeatureInfo)
    }
  }

  /**
   * Check if column expression and column configuration both do not exist in input feature information.
   * Add default column expression value with the name in output tensor information when both do not exist
   *
   * @param inputFeatureInfo Input feature information
   * @param outputTensorInfo Output tensor information
   * @return Optional input feature information
   */
  private def checkIfColumnExprAndConfigBothNotExist(inputFeatureInfo: InputFeatureInfo, outputTensorInfo: OutputTensorInfo): Option[InputFeatureInfo] = {

    inputFeatureInfo.columnConfig match {
      case Some(_) => Some(inputFeatureInfo)
      case None => Some(InputFeatureInfo(columnExpr = Some(outputTensorInfo.name), None, inputFeatureInfo.transformConfig))
    }
  }

  /**
   * Check the shape in output tensor information.
   * Add default shape value of an empty array of integer if shape does not exist in output tensor information
   *
   * @param feature Feature in TensorizeIn configuration
   * @return Feature in TensorizeIn configuration
   */
  private def checkShape(feature: Feature): Feature = {

    feature.outputTensorInfo.shape match {
      case Some(_) => feature
      case None => Feature(feature.inputFeatureInfo, OutputTensorInfo(feature.outputTensorInfo.name, feature.outputTensorInfo.dtype, Some(Array.empty[Int])))
    }
  }
}