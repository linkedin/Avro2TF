package com.linkedin.avro2tf.configs

import io.circe.generic.extras.Configuration
import io.circe.{Decoder, Encoder}

/**
 * Implicits for Circe.
 *
 * Includes Configuration object and encoders/decoders for the enums,
 * which Circe doesn't derive automatically.
 *
 * NOTE: Intellij does not always correctly pick up that `import JsonCodecs._`
 * is being used, even when it is.
 */
object JsonCodecs {
  implicit val customConfig: Configuration = Configuration.default.withDefaults

  implicit val combinerTypeDecoder: Decoder[Combiner.Value] = Decoder.enumDecoder(Combiner)
  implicit val dataTypeDecoder: Decoder[DataType.Value] = Decoder.enumDecoder(DataType)

  implicit val dataTypeEncoder: Encoder[DataType.Value] = Encoder.enumEncoder(DataType)
  implicit val combinerTypeEncoder: Encoder[Combiner.Value] = Encoder.enumEncoder(Combiner)
}
