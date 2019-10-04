package com.linkedin.avro2tf.constants

/**
 * Param names for the PrepRanking job
 */
object PrepRankingJobParamNames {
  final val INPUT_DATA_PATH = "input-data-path"
  final val INPUT_METADATA_PATH = "input-metadata-path"
  final val OUTPUT_DATA_PATH = "output-data-path"
  final val OUTPUT_METADATA_PATH = "output-metadata-path"
  final val GROUP_ID_LIST = "group-id-list"
  final val GROUP_LIST_MAX_SIZE = "group-list-max-size"
  final val ENABLE_FILTER_ZERO = "enable-filter-zero"
  final val DROP_COLUMN_LIST = "drop-column-list"
  final val NUM_OUTPUT_FILES = "num-output-files"
  final val SHUFFLE = "shuffle"
  final val EXECUTION_MODE = "execution-mode"
  final val LABEL_PADDING_VALUE = "label-padding-value"
  final val FEATURE_PADDING_VALUE = "feature-padding-value"
  final val SKIP_PADDING = "skip-padding"
}
