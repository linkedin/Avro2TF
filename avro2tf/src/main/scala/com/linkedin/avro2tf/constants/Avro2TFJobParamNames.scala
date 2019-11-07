package com.linkedin.avro2tf.constants

/**
 * Param name Constants used by in job param parser
 *
 */
object Avro2TFJobParamNames {
  final val INPUT_PATHS = "input-paths"
  final val WORKING_DIR = "working-dir"
  final val INPUT_DATE_RANGE = "input-date-range"
  final val INPUT_DAYS_RANGE = "input-days-range"
  final val NUM_OUTPUT_FILES = "num-output-files"
  final val MIN_PARTS = "min-parts"
  final val SHUFFLE = "shuffle"
  final val EXTERNAL_FEATURE_LIST_PATH = "external-feature-list-path"
  final val AVRO2TF_CONFIG_PATH = "avro2tf-config-path"
  final val TRAIN_MODE = "train-mode"
  final val EXECUTION_MODE = "execution-mode"
  final val ENABLE_CACHE = "enable-cache"
  final val SKIP_CONVERSION = "skip-conversion"
  final val OUTPUT_FORMAT = "output-format"
  final val EXTRA_COLUMNS_TO_KEEP = "extra-columns-to-keep"
  final val TENSORS_SHARING_FEATURE_LISTS = "tensors-sharing-feature-lists"
  final val NUM_PARTITIONS = "num-partitions"
  final val PARTITION_FIELD_NAME = "partition-field-name"
  final val ENABLE_TERM_ONLY_FEATURE_LIST = "enable-term-only-feature-list"
  final val DISCARD_UNKNOWN_ENTRIES = "discard-unknown-entries"
  final val ENABLE_FILTER_ZERO = "enable-filter-zero"
  final val PASS_THROUGH_ONLY = "pass-through-only"
}
