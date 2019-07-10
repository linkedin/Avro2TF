package com.linkedin.avro2tf.parsers

import scala.collection.JavaConverters._
import scala.util.Try

import cats.data.NonEmptyList
import com.linkedin.avro2tf.configs._
import com.typesafe.config.ConfigFactory
import io.circe
import io.circe.Errors
import io.circe.config.syntax._
import io.circe.generic.extras.auto._

/**
 * Parser file for TensorizeIn configuration
 */
object TensorizeInConfigParser {

  /**
   * Get the TensorizeIn configuration with sanity check
   *
   * @param configString HOCON format of TensorizeIn configuration
   * @return TensorizeIn configuration
   */
  def getTensorizeInConfiguration(configString: String): TensorizeInConfiguration = {
    import com.linkedin.avro2tf.configs.JsonCodecs._

    val rawConfig = ConfigFactory.parseString(configString)

    val featureConfig = rawConfig.getConfigList("features").asScala.map(_.as[Feature])
    // label field is optional, so wrap it in a Try first
    val labelConfig = Try(rawConfig.getConfigList("labels")).map(_.asScala.map(_.as[Feature])).toOption

    // check that we parsed the features correctly
    val errorList: Option[NonEmptyList[circe.Error]] = NonEmptyList.fromList(
      (featureConfig ++ labelConfig.getOrElse(List()))
        .filter(_.isLeft).map(_.left.get).toList)
    errorList.foreach(es => throw new Exception(es.map(_.getMessage).toList.mkString("\n"), Errors(es)))

    // since we didn't throw any errors, we know everything is a Right
    val features = featureConfig.map(_.right.get)
    val labels = labelConfig.map(_.map(_.right.get))

    TensorizeInConfiguration(
      features = sanityCheck(features),
      labels = sanityCheck(labels.getOrElse(Seq.empty))
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

    checkedItems
  }

  /**
   * Check column expression and column configuration in input feature information.
   * Add default column expression value with the name in output tensor information if input feature information does not exist
   *
   * @param feature Feature in TensorizeIn configuration
   * @return Feature in TensorizeIn configuration
   */
  private def checkColumnExprAndConfig(feature: Feature): Feature = {

    val updatedInputFeatureInfo: Option[InputFeatureInfo] = (feature.inputFeatureInfo match {
      case Some(inputFeatureInfo) =>
        inputFeatureInfo.columnExpr.fold(
          checkIfColumnExprAndConfigBothNotExist(inputFeatureInfo, feature.outputTensorInfo))(
          checkIfColumnExprAndConfigBothExist(inputFeatureInfo, _))
      case None => Some(InputFeatureInfo(columnExpr = Some(feature.outputTensorInfo.name), None, None))
    }).map(normalizeTransform)

    feature.copy(inputFeatureInfo = updatedInputFeatureInfo)
  }

  /** In the event the transform config specified neither hashInfo nor tokenization,
   * convert the [[InputFeatureInfo]]'s transform config to None.
   *
   * @param inputFeatureInfo - the [[InputFeatureInfo]] of a [[Feature]] in the TensorizeIn configuration
   * @return the input with transform config normalized
   */
  private def normalizeTransform(inputFeatureInfo: InputFeatureInfo): InputFeatureInfo = {

    val updatedTransform = inputFeatureInfo.transformConfig match {
      case Some(TransformConfig(None, None)) => None
      case _ => inputFeatureInfo.transformConfig
    }

    inputFeatureInfo.copy(transformConfig = updatedTransform)
  }

  /**
   * Check if column expression and column configuration both exist in input feature information.
   * Throw an exception when column expression and column configuration both exist in input feature information
   *
   * @param inputFeatureInfo Input feature information
   * @param columnExpr Column expression in input feature information
   * @return Optional input feature information
   */
  private def checkIfColumnExprAndConfigBothExist(
    inputFeatureInfo: InputFeatureInfo,
    columnExpr: String): Option[InputFeatureInfo] = {

    inputFeatureInfo.columnConfig match {
      case Some(columnConfig) => throw new IllegalArgumentException(
        s"Column expression $columnExpr and column configuration" +
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
  private def checkIfColumnExprAndConfigBothNotExist(
    inputFeatureInfo: InputFeatureInfo,
    outputTensorInfo: OutputTensorInfo): Option[InputFeatureInfo] = {

    inputFeatureInfo.columnConfig match {
      case Some(_) => Some(inputFeatureInfo)
      case None => Some(
        InputFeatureInfo(
          columnExpr = Some(outputTensorInfo.name),
          None,
          inputFeatureInfo.transformConfig))
    }
  }
}