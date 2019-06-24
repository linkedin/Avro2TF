package com.linkedin.avro2tf.parsers


import cats.data.NonEmptyList
import com.linkedin.avro2tf.configs.{Feature, InputFeatureInfo, OutputTensorInfo, TensorizeInConfiguration}
import com.typesafe.config.{Config, ConfigFactory}
import io.circe
import io.circe.Errors
import io.circe.generic.auto._
import io.circe.config.syntax._

import collection.JavaConverters._
import scala.util.Try

/**
  * Parser file for TensorizeIn configuration
  *
  */
object TensorizeInConfigParser {

  /**
   * Get the TensorizeIn configuration with sanity check
   *
   * @param configString HOCON format of TensorizeIn configuration
   * @return TensorizeIn configuration
   */
  def getTensorizeInConfiguration(configString: String): TensorizeInConfiguration = {

    val rawConfig = ConfigFactory.parseString(configString)
    val featureConfig = rawConfig.getConfigList("features").asScala.map(parseFeature)
    val labelConfig = Try(rawConfig.getConfigList("labels")).map(_.asScala.map(parseFeature)).toOption

    // check that we parsed the features correctly
    val errorList = NonEmptyList.fromList((featureConfig ++ labelConfig.getOrElse(List()))
      .filter(_.isLeft).map(_.left.get).toList)
    errorList.foreach(es => throw Errors(es).fillInStackTrace())

    // since we didn't throw any errors, we know everything is a Right
    val features = featureConfig.map(_.right.get)
    val labels = labelConfig.map(_.map(_.right.get))

    TensorizeInConfiguration(
      features = sanityCheck(features),
      labels = Some(sanityCheck(labels.getOrElse(Seq.empty)))
    )
  }

  /** Parse the feature configs.
    *
    * @param featureConfig - the [[Config]] object for an individual [[Feature]]
    * @return [[Either]] of a [[circe.Error]] or a [[Feature]]
    */
  private def parseFeature(featureConfig: Config): Either[circe.Error, Feature] = {
    val outputTensorInfo: Either[circe.Error, OutputTensorInfo] =
      featureConfig.getConfig("outputTensorInfo").as[OutputTensorInfo]

    val inputFeatureInfo = if (featureConfig.hasPath("inputFeatureInfo"))
      Some(parseInputFeatureInfo(featureConfig.getConfig("inputFeatureInfo")))
    else None

    outputTensorInfo.fold(e => Left(e), oti => Right(Feature(inputFeatureInfo, oti)))
  }

  /**
    *
    * @param inputFeatureInfoConfig
    * @return
    */
  private def parseInputFeatureInfo(inputFeatureInfoConfig: Config): InputFeatureInfo = {
    val convertToStringSeqMap = (a: AnyRef) => a.asInstanceOf[java.util.Map[String, java.util.List[String]]]
    val convertToStringAnyMap = (a: AnyRef) => a.asInstanceOf[java.util.Map[String, Any]].asScala.toMap

    val columnExpr = Try(inputFeatureInfoConfig.getString("columnExpr")).toOption
    val columnConfig: Option[Map[String, Map[String, Seq[String]]]] = Try(inputFeatureInfoConfig.getConfig("columnConfig"))
      .map(c =>
        c.root().unwrapped().asScala.toMap
          // we need to use map here because mapValues is lazy and this will nee to be serialized for Spark
          .map { case (k, x) => k -> convertToStringSeqMap(x).asScala.toMap
          .map { case (j, y) => j -> y.asScala }
        }
      ).toOption
    val transformConfig: Option[Map[String, Map[String, Any]]] = Try(inputFeatureInfoConfig.getConfig("transformConfig"))
      .map { config =>
        config.root().unwrapped().asScala.toMap
          .mapValues(convertToStringAnyMap)
      }.toOption

    InputFeatureInfo(columnExpr, columnConfig, transformConfig)
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