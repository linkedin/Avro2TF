package com.linkedin.avro2tf.configs

import java.util.Objects

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration

/**
 * Enumeration to represent different data type of tensors
 */
object DataType extends Enumeration {
  type DataType = Value
  val string, int, long, double, float, byte, boolean = Value
}

class DataTypeRef extends TypeReference[DataType.type]

/**
 * Enumeration to represent combiner options to deal with hash collision
 */
object Combiner extends Enumeration {
  type Combiner = Value
  val SUM, AVG, MAX = Value
}

class CombinerTypeRef extends TypeReference[Combiner.type]

/**
 * Case class for hashing info
 *
 * @param hashBucketSize The number of buckets of each hash function
 * @param numHashFunctions The number of hash functions to use - defaults to 1
 * @param combiner Which combiner to use when collision happens
 */
case class HashInfo(
  hashBucketSize: Int,
  numHashFunctions: Int = 1,
  @JsonScalaEnumeration(classOf[CombinerTypeRef]) combiner: Combiner.Combiner = Combiner.SUM
)

/**
 * Case class for Tokenization config
 *
 * @param removeStopWords - whether to remove stop words, defaults to false
 */
case class Tokenization(removeStopWords: Boolean = false)

/**
 * Case class for Transform config.
 *
 * @param hashInfo - optional [[HashInfo]] config.
 * @param tokenization - optional [[Tokenization]] config.
 */
case class TransformConfig(hashInfo: Option[HashInfo], tokenization: Option[Tokenization])

/**
 * Case class for TensorizeIn configuration
 *
 * @param features A sequence of features
 * @param labels A sequence of labels, which may be empty
 */
case class TensorizeInConfiguration(
  features: Seq[Feature],
  labels: Seq[Feature] = Seq()
) {
  require(
    features.nonEmpty,
    s"TensorizeIn configuration must have a non-empty sequence of features."
  )
}

/**
 * Case class for Feature in TensorizeIn configuration
 *
 * @param inputFeatureInfo Optional input feature information
 * @param outputTensorInfo Output tensor information
 */
case class Feature(
  inputFeatureInfo: Option[InputFeatureInfo],
  outputTensorInfo: OutputTensorInfo) {

  /** Whether this feature defines a hash transform
   */
  def hasHashTransform: Boolean = inputFeatureInfo.exists(_.transformConfig.exists(_.hashInfo.isDefined))

  /** Whether this feature defines a tokenization transform
   */
  def hasTokenizationTransform: Boolean = inputFeatureInfo.exists(_.transformConfig.exists(_.tokenization.isDefined))
}

/**
 * Case class for Input Feature Information in TensorizeIn configuration
 *
 * @param columnExpr Optional column expression
 * @param columnConfig Optional column configuration
 * @param transformConfig Optional transformation configuration
 */
case class InputFeatureInfo(
  columnExpr: Option[String],
  columnConfig: Option[Map[String, Map[String, Seq[String]]]],
  transformConfig: Option[TransformConfig])

/**
 * Case class for Output Tensor Information in TensorizeIn configuration
 *
 * @param name Name of tensor
 * @param dtype Data type of tensor
 * @param shape Shape of tensor
 */
case class OutputTensorInfo(
  name: String,
  @JsonScalaEnumeration(classOf[DataTypeRef]) dtype: DataType.DataType,
  shape: Seq[Int] = Seq(),
  isSparse: Boolean = false,
  isDocumentFeature: Boolean = true) {

  // Array is a Java array, so we need to implement equals
  override def equals(that: Any): Boolean =
    that match {
      case that: OutputTensorInfo =>
        that.canEqual(this) &&
          this.name == that.name &&
          this.dtype == that.dtype &&
          this.shape.sameElements(that.shape)
      case _ => false
    }

  override def hashCode(): Int =
    Objects.hash(name, dtype.toString, shape)
}