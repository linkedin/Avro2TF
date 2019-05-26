package com.linkedin.avro2tf.utils

object TrainingMode extends Enumeration {
  type TrainingMode = Value
  val training, validation, test = Value
}
