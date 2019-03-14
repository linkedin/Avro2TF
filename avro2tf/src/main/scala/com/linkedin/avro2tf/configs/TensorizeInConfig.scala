package com.linkedin.avro2tf.configs

/**
 * Enumeration to represent different data type of tensors
 */
object DataType extends Enumeration {
  type DataType = Value
  val sparseVector, string, int, long, double, float, byte = Value
}

/**
 * Enumeration to represent combiner options to deal with hash collision
 */
object CombinerType extends Enumeration {
  type CombinerType = Value
  val SUM, AVG, MAX = Value
}

/**
 * Case class for hashing info
 *
 * @param hashBucketSize The number of buckets of each hash function
 * @param numHashFunctions The number of hash functions to use
 * @param combinerType Which combiner to use when collision happens
 */
case class HashInfo(
  hashBucketSize: Int,
  numHashFunctions: Int = 1,
  combinerType: CombinerType.CombinerType = CombinerType.SUM
)

/**
 * Case class for TensorizeIn configuration
 *
 * @param features A sequence of features
 * @param labels An optional sequence of labels
 */
case class TensorizeInConfiguration(
  features: Seq[Feature],
  labels: Option[Seq[Feature]]
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
  outputTensorInfo: OutputTensorInfo
)

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
  transformConfig: Option[Map[String, Map[String, Any]]]
)

/**
 * Case class for Output Tensor Information in TensorizeIn configuration
 *
 * @param name Name of tensor
 * @param dtype Data type of tensor
 * @param shape Optional shape of tensor
 */
case class OutputTensorInfo(
  name: String,
  dtype: String,
  shape: Option[Array[Int]]) {

  // Add an additional DataType format value for case matching
  val dataType: DataType.Value = DataType.withName(dtype)
}