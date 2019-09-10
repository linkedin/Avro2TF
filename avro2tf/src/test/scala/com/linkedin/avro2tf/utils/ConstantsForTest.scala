package com.linkedin.avro2tf.utils

import com.linkedin.avro2tf.configs.DataType

/**
 * Constants used by Avro2T tests
 *
 */
object ConstantsForTest {
  final val AVRO_RECORD = "avro"
  final val ENABLE_APPEND = false
  final val ENABLE_CACHE_VALUE = false
  final val ENABLE_RECURSIVE = true
  final val ENABLE_SHUFFLE_VALUE = false
  final val EXTRA_COLUMNS_TO_KEEP_VALUE = "review,words.term:terms"
  final val FEATURE_FIRST_WORD_COL_NAME = "firstWord"
  final val FEATURE_WORD_SEQ_COL_NAME = "wordSeq"
  final val FEATURE_WORD_SEQ_HASHED_COL_NAME = "wordSeq_hashed"
  final val FEATURE_WORDS_WIDE_FEATURES_HASH_COL_NAME = "words_wideFeatures_hash"
  final val FEATURE_WORDS_WIDE_FEATURES_DENSE_COL_NAME = "words_wideFeatures_dense"
  final val FEATURE_WORDS_WIDE_FEATURES_SPARSE_COL_NAME = "words_wideFeatures_sparse"
  final val FEATURE_LIST_SHARING_EXPECTED_VALUE_PATH_CASE1 = "data/share_feat_list/feat_lists_case_1"
  final val FEATURE_LIST_SHARING_EXPECTED_VALUE_PATH_CASE2 = "data/share_feat_list/feat_lists_case_2"
  final val INPUT_DATE_RANGE_VALUE = "20190101-20190117"
  final val INPUT_DATE_RANGE_VALUE_START = "20190101"
  final val INPUT_DATE_RANGE_VALUE_END = "20190117"
  final val INPUT_DAYS_RANGE_VALUE = "6-3"
  final val INPUT_MOVIELENS_FILE_PATHS = "src/test/resources/data/trainSmall.avro"
  final val INPUT_PATHS_VALUE = "/input/123"
  final val INPUT_TEXT_FILE_PATHS = "src/test/resources/data/text.avro"
  final val INPUT_SHARE_FEATURE_PATH = "src/test/resources/data/share_feat_list/test_share_feature_list.avro"
  final val MIN_PARTS_VALUE = 100
  final val MIX_NTV_FEATURE_NAME = "fruits_day4"
  final val NUM_OUTPUT_FILES_VALUE = 3
  final val SAMPLE_EXTERNAL_FEATURE_LIST = "a\nb\nc\n"
  final val SKIP_CONVERSION_VALUE = "false"
  final val SPARK_DRIVER_BIND_ADDRESS_NAME = "spark.driver.bindAddress"
  final val SPARK_DRIVER_BIND_ADDRESS_VALUE = "127.0.0.1"
  final val SPARK_SESSION_BUILDER_MASTER = "local[*]"
  final val AVRO2TF_CONFIG_PATH_VALUE_2 = "configs/avro2tf_config_2.json"
  final val AVRO2TF_CONFIG_PATH_VALUE_2_HOCON = "configs/avro2tf_config_2.conf"
  final val AVRO2TF_CONFIG_PATH_VALUE_3 = "configs/avro2tf_config_3.json"
  final val AVRO2TF_CONFIG_PATH_VALUE_4 = "configs/avro2tf_config_4.json"
  final val AVRO2TF_CONFIG_PATH_VALUE_5 = "configs/avro2tf_config_5.json"
  final val AVRO2TF_CONFIG_PATH_VALUE_MOVIELENS = "configs/avro2tf_config_movielens.json"
  final val AVRO2TF_CONFIG_PATH_VALUE_MOVIELENS_RANK = "configs/avro2tf_config_movielens_rank.json"
  final val AVRO2TF_CONFIG_PATH_VALUE_SAMPLE = "configs/avro2tf_config_sample.json"
  final val AVRO2TF_CONFIG_PATH_VALUE_SAMPLE_WITHOUT_INT_FEATURES =
    "configs/avro2tf_config_without_int_features.json"
  final val AVRO2TF_CONFIG_PATH_VALUE_SHARE_FEATURE = "configs/avro2tf_config_share_feature.json"
  final val AVRO2TF_CONFIG_TEST_ARRAY = Array.empty[Int]
  final val AVRO2TF_CONFIG_TEST_INFO = "testInfo"
  final val AVRO2TF_CONFIG_TEST_VALUE = "test_value"
  final val AVRO2TF_CONFIG_TEST_LONG_VALUE = DataType.long
  final val TENSORS_SHARING_FEATURE_LISTS_VALUE_CASE_1 = "comments_after,comments_before; " +
    "fruits_day1,fruits_day2,fruits_day3;receive_platform,send_platform"
  final val TENSORS_SHARING_FEATURE_LISTS_VALUE_CASE_2 = "comments_after,comments_before; " +
    "fruits_day1,fruits_day2;receive_platform,send_platform"
  final val TENSORS_SHARING_FEATURE_LISTS_VALUE_CASE_3 = "comments_after,comments_before; " +
    "fruits_day1,fruits_day4;receive_platform,send_platform"
  final val TF_RECORD = "tfrecord"
  final val WORKING_DIRECTORY_VALUE = "/output/456"
  final val WORKING_DIRECTORY_FEATURE_EXTRACTION_TEXT = "/tmp/feature-extraction-test/text"
  final val WORKING_DIRECTORY_FEATURE_TRANSFORMATION_TEXT = "/tmp/feature-transformation-test/text"
  final val WORKING_DIRECTORY_HASH_TRANSFORMATION = "/tmp/feature-transformation-test/hashing"
  final val WORKING_DIRECTORY_FEATURE_LIST_GENERATION_TEXT = "/tmp/feature-list-generation-test-text"
  final val WORKING_DIRECTORY_TENSOR_METADATA_GENERATION_TEXT = "/tmp/tensor-metadata-generation-test-text"
  final val WORKING_DIRECTORY_INDICES_CONVERSION = "/tmp/indices-conversion-test"
  final val WORKING_DIRECTORY_AVRO2TF = "/tmp/avro2tf-test"
  final val WORKING_DIRECTORY_AVRO2TF_MOVIELENS = "/tmp/avro2tf-test-movielens"
  final val WORKING_DIRECTORY_PARTITION_TEST = "/tmp/partition-test"
  final val EXPECTED_FEATURE_LIST_W_COUNTS_PATH = "data/share_feat_list/feat_lists_w_counts"
  final val EXPECTED_TENSOR_METADATA_GENERATED_JSON_PATH_TEXT = "metadata/tensor_metadata_text_1.json"
  final val EXPECTED_TENSOR_METADATA_WITH_PARTITION_ID = "metadata/tensor_metadata_with_partition_id.json"
  final val EXPECTED_TENSOR_METADATA_WITHOUT_INT_FEATURES_GENERATED_JSON_PATH_TEXT =
    "metadata/tensor_metadata_text_without_int_features.json"
  final val EXTERNAL_FEATURE_LIST_FILE_NAME_MOVIELENS = "genreFeatures_term"
  final val EXTERNAL_FEATURE_LIST_FILE_NAME_TEXT = "words_term"
  final val EXTERNAL_FEATURE_LIST_PATH_TEXT = "/external-feature-list-text"
  final val EXTERNAL_FEATURE_LIST_PATH_VALUE = "/tmp/external-feature-list"
  final val SPARSE_VECTOR_INDICES_FIELD_NAME = "indices"
  final val SPARSE_VECTOR_VALUES_FIELD_NAME = "values"
}