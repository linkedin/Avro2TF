package com.linkedin.avro2tf.configs

/**
 * Case class for the TensorizeIn tensor metadata
 *
 * @param features Tensor metadata of a sequence of features
 * @param labels Tensor metadata of a sequence of labels
 */
case class TensorizeInTensorMetadata(features: Seq[TensorMetadata], labels: Seq[TensorMetadata])

/**
 * Case class for the tensor metadata
 *
 * @param name Name of a tensor
 * @param dtype Data type of a tensor
 * @param shape Shape of a tensor
 * @param cardinality Optional cardinality of a tensor
 * @param isSparse If it is a sparse tensor
 */
case class TensorMetadata(
  name: String,
  dtype: String,
  shape: Seq[Int],
  cardinality: Option[Any],
  isSparse: Boolean = false
)