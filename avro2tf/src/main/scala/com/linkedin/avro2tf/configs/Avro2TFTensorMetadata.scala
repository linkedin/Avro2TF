package com.linkedin.avro2tf.configs

import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import com.linkedin.avro2tf.configs.DataType.DataType

/**
 * Case class for the Avro2TF tensor metadata
 *
 * @param features Tensor metadata of a sequence of features
 * @param labels Tensor metadata of a sequence of labels
 */
case class Avro2TFTensorMetadata(features: Seq[TensorMetadata], labels: Seq[TensorMetadata])

/**
 * Case class for the tensor metadata
 *
 * @param name Name of a tensor
 * @param dtype Data type of a tensor
 * @param shape Shape of a tensor
 * @param cardinality Optional cardinality of a tensor
 * @param isSparse If it is a sparse tensor
 * @param isDocumentFeature If it is a document feature
 */
case class TensorMetadata(
  name: String,
  @JsonScalaEnumeration(classOf[DataTypeRef]) dtype: DataType,
  shape: Seq[Int],
  cardinality: Option[Long],
  isSparse: Boolean = false,
  isDocumentFeature: Boolean = true
)
