package com.linkedin.avro2tf.parsers

import java.io.File

import scala.io.Source

import com.linkedin.avro2tf.configs.Avro2TFConfiguration
import com.linkedin.avro2tf.constants.Avro2TFJobParamNames
import com.linkedin.avro2tf.constants.Constants._
import com.linkedin.avro2tf.utils.{IOUtils, TrainingMode}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.JobConf

/**
 * Avro2TF parsed parameters will be put into this case class for ease of access
 *
 * @param inputPaths A list of input paths
 * @param workingDir All intermediate and final results will be written to this directory
 * @param inputDateRange Date range for daily structured input path
 * @param inputDaysRange Days offset range for daily structured input path
 * @param numOfOutputFiles The number of files written to HDFS
 * @param minParts Minimum number of partitions for input data
 * @param enableShuffle Whether to enable shuffling the final output data
 * @param externalFeaturesListPath Path of external feature list supplied by the user
 * @param avro2TFConfig Avro2TF configuration for features and labels to be tensorized
 * @param executionMode One of "train", "validate" or "test"
 * @param enableCache Whether to enable caching the intermediate Spark DataFrame result
 * @param skipConversion Indicate whether to skip the conversion step
 * @param outputFormat Output format of tensorized data, e.g. Avro or TFRecord
 * @param enableFilterZero Filter out zeros in Sparse vector output. Once it's turned on, it will be applied to all sparse vector output
 * @param passThroughOnly Whether to pass through inputs and only change outputs formats, num partitions, etc.
 * @param featureListCap A map of feature name to its max feature list size. Useful for doing subset. The feature entries with top frequencies will be kept.
 */
case class Avro2TFParams(
  inputPaths: Seq[String],
  workingDir: WorkingDirectory,
  inputDateRange: Seq[String],
  inputDaysRange: Seq[Int],
  numOfOutputFiles: Int,
  minParts: Int,
  enableShuffle: Boolean,
  externalFeaturesListPath: String,
  avro2TFConfig: Avro2TFConfiguration,
  executionMode: TrainingMode.TrainingMode,
  enableCache: Boolean,
  skipConversion: Boolean,
  outputFormat: String,
  extraColumnsToKeep: Seq[String],
  tensorsSharingFeatureLists: Array[Array[String]],
  numPartitions: Int,
  partitionFieldName: String,
  termOnlyFeatureList: Boolean,
  discardUnknownEntries: Boolean,
  enableFilterZero: Boolean,
  passThroughOnly: Boolean,
  featureListCap: Map[String, Int]
)

/**
 * A wrapping of paths under working directory
 *
 * @param rootPath The root output path which stores all intermediate and final results
 */
case class WorkingDirectory(rootPath: String) {

  val trainingDataPath = s"$rootPath/$TRAINING_DATA_DIR_NAME"
  val validationDataPath = s"$rootPath/$VALIDATION_DATA_DIR_NAME"
  val testDataPath = s"$rootPath/$TEST_DATA_DIR_NAME"
  val featureListPath = s"$rootPath/$FEATURE_LIST_DIR_NAME"
  val termOnlyFeatureListPath = s"$rootPath/$TERM_ONLY_FEATURE_LIST_DIR_NAME"
  val tensorMetadataPath = s"$rootPath/$METADATA_DIR_NAME/$TENSOR_METADATA_FILE_NAME"
}

/**
 * Parser file for Avro2TF job parameters from command line arguments
 */
object Avro2TFJobParamsParser {

  /**
   * Parser to parse Avro2TF job parameters
   */
  private val parser = new scopt.OptionParser[Avro2TFParams](
    "Parsing command line for Avro2TF job") {

    // Parse a list of comma separated paths for input
    opt[Seq[String]](Avro2TFJobParamNames.INPUT_PATHS)
      .action((inputPaths, avro2TFParams) => avro2TFParams.copy(inputPaths = inputPaths))
      .required()
      .text(
        """Required.
          |A list of comma separated paths for input.""".stripMargin
      )

    // Parse the path to working directory where the output should be saved
    opt[String](Avro2TFJobParamNames.WORKING_DIR)
      .action((workingDir, avro2TFParams) => avro2TFParams.copy(workingDir = WorkingDirectory(workingDir.trim)))
      .required()
      .text(
        """Required.
          |The path to working directory where the output should be saved.""".stripMargin
      )

    // Parse the input date range in the format of yyyymmdd-yyyymmdd
    opt[String](Avro2TFJobParamNames.INPUT_DATE_RANGE)
      .action(
        (intputDateRange, avro2TFParams) => {
          val dates = intputDateRange.trim.split('-')
          require(dates.length == 2, "must have start and end date")
          avro2TFParams.copy(inputDateRange = dates.map(_.trim))
        }
      )
      .text(
        """Optional.
          |The input date range in the format of yyyymmdd-yyyymmdd.""".stripMargin
      )

    // Parse the input days range in the format of startOffest-endOffset
    opt[String](Avro2TFJobParamNames.INPUT_DAYS_RANGE)
      .action(
        (inputDaysRange, avro2TFParams) => {
          val daysOffset = inputDaysRange.trim.split('-')
          require(daysOffset.length == 2, "must have start and end days offset")
          val intDaysOffset = daysOffset.map(_.trim.toInt)
          require(intDaysOffset.forall(_ >= 0), s"days-range can not be negative value: $inputDaysRange")
          avro2TFParams.copy(inputDaysRange = intDaysOffset)
        }
      )
      .text(
        """Optional.
          |The input days range in the format of startOffest-endOffset.""".stripMargin
      )

    // Parse the number of output files with the default set to -1
    opt[Int](Avro2TFJobParamNames.NUM_OUTPUT_FILES)
      .action((numOfOutputFiles, avro2TFParams) => avro2TFParams.copy(numOfOutputFiles = numOfOutputFiles))
      .optional()
      .text(
        """Optional.
          |The number of output files with the default set to -1.""".stripMargin
      )

    // Parse the minimum number of partitions for input data; if below this threshold, repartition will be triggered
    opt[Int](Avro2TFJobParamNames.MIN_PARTS)
      .action(
        (minParts, avro2TFParams) => {
          require(minParts > 0, "min-parts must be greater than 0")
          avro2TFParams.copy(minParts = minParts)
        }
      )
      .optional()
      .text(
        """Optional.
          |The minimum number of partitions for input data; if below this threshold, repartition will be triggered."""
          .stripMargin
      )

    // Parse whether to shuffle the converted training data with the default set to true
    opt[Boolean](Avro2TFJobParamNames.SHUFFLE)
      .action((enableShuffle, avro2TFParams) => avro2TFParams.copy(enableShuffle = enableShuffle))
      .optional()
      .text(
        """Optional.
          |Whether to shuffle the converted training data with the default set to true.""".stripMargin
      )

    // Parse the path to external feature list where the user supplied feature metadata is written
    opt[String](Avro2TFJobParamNames.EXTERNAL_FEATURE_LIST_PATH)
      .action(
        (externalFeatureListPath, avro2TFParams) => avro2TFParams
          .copy(externalFeaturesListPath = externalFeatureListPath.trim))
      .text(
        """Optional.
          |The path to external feature list where the user supplied feature metadata is written.""".stripMargin
      )

    // Parse the Avro2TF configuration in JSON format
    opt[String](Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH)
      .action(
        (avro2TFConfigPath, avro2TFParams) => {

          var jsonInput = ""

          // Read JSON config from local if exists, otherwise read from HDFS
          if (new File(avro2TFConfigPath).exists()) {
            val bufferedSource = Source.fromFile(avro2TFConfigPath)
            jsonInput = bufferedSource.mkString
            bufferedSource.close()
          } else {
            val fs = FileSystem.get(new JobConf())
            val path = new Path(avro2TFConfigPath)
            if (fs.exists(path)) {
              jsonInput = IOUtils.readContentFromHDFS(fs, path)
              fs.close()
            } else {
              throw new IllegalArgumentException(
                s"Specified avro2tf config path: $avro2TFConfigPath does not exist in either local or HDFS"
              )
            }
          }

          // Get the Avro2TF Configuration
          val avro2TFConfiguration = Avro2TFConfigParser.getAvro2TFConfiguration(jsonInput)

          avro2TFParams.copy(avro2TFConfig = avro2TFConfiguration)
        }
      )
      .optional()
      .text(
        """Optional.
          |The Avro2TF configuration in JSON format.""".stripMargin
      )

    // Parse the execution mode, which decides whether to prepare training, validation, or test data
    opt[String](Avro2TFJobParamNames.EXECUTION_MODE)
      .action(
        (executionMode, avro2TFParams) =>
          avro2TFParams.copy(executionMode = TrainingMode.withName(executionMode.toLowerCase))
      )
      .optional()
      .text(
        """Optional.
          |Whether to prepare training, validation, or test data.""".stripMargin
      )

    // Parse whether to cache the intermediate Spark DataFrame result with default set to false
    opt[Boolean](Avro2TFJobParamNames.ENABLE_CACHE)
      .action((enableCache, avro2TFParams) => avro2TFParams.copy(enableCache = enableCache))
      .optional()
      .text(
        """Optional.
          |Whether to cache the intermediate Spark DataFrame result with default set to false.""".stripMargin
      )

    // Parse whether to skip the conversion step with default set to false
    opt[Boolean](Avro2TFJobParamNames.SKIP_CONVERSION)
      .action((skipConversion, avro2TFParams) => avro2TFParams.copy(skipConversion = skipConversion))
      .optional()
      .text(
        """Optional.
          |Whether to skip the conversion step with default set to false.""".stripMargin
      )

    // Parse the output format of tensorized data, e.g. Avro or TFRecord
    opt[String](Avro2TFJobParamNames.OUTPUT_FORMAT)
      .action((outputFormat, avro2TFParams) => avro2TFParams.copy(outputFormat = outputFormat))
      .optional()
      .text(
        """Optional.
          |The output format of tensorized data, e.g. Avro or TFRecord.""".stripMargin
      )

    opt[Seq[String]](Avro2TFJobParamNames.EXTRA_COLUMNS_TO_KEEP)
      .action((extraColumns, avro2TFParams) => avro2TFParams.copy(extraColumnsToKeep = extraColumns))
      .optional()
      .text(
        """Optional.
          |A list of comma separated column names to specify extra columns to keep.""".stripMargin
      )

    opt[String](Avro2TFJobParamNames.TENSORS_SHARING_FEATURE_LISTS)
      .valueName("<tensor>,...,<tensor>;<tensor>,...,<tensor>")
      .action(
        (tensors, avro2TFParams) => {
          val tensor_array = tensors.trim().split(";").map(_.split(",").map(_.trim()))
          if (tensor_array.size == 0 || !tensor_array.forall(_.size > 1)) {
            throw new IllegalArgumentException(
              s"Each group in the feature list sharing setting must have at least 1 tensor.\n${
                tensor_array
                  .mkString("; ")
              }")
          }
          val tensors_flatten = tensor_array.flatten
          if (tensors_flatten.distinct.size != tensors_flatten.size) {
            throw new IllegalArgumentException(
              s"Different shared feature list groups can not have overlapping tensors.\n${
                tensor_array
                  .mkString("; ")
              }")
          }
          avro2TFParams.copy(tensorsSharingFeatureLists = tensor_array)
        }
      )
      .optional()
      .text(
        """Optional.
          |Groups of output tensor names separated by semicolon; tensors in the same group are separated by comma.
          |Tensors within the same group share the same feature list."""
          .stripMargin
      )

    opt[Int](Avro2TFJobParamNames.NUM_PARTITIONS)
      .action((numPartitions, avro2TFParams) => avro2TFParams.copy(numPartitions = numPartitions))
      .optional()
      .text(
        """Optional.
          |The number of partitions""".stripMargin
      )

    opt[String](Avro2TFJobParamNames.PARTITION_FIELD_NAME)
      .action(
        (partitionFieldName, avro2TFParams) => avro2TFParams
          .copy(partitionFieldName = partitionFieldName.trim))
      .optional()
      .text(
        """Optional.
          |The field name to apply partition.""".stripMargin
      )

    opt[Boolean](Avro2TFJobParamNames.ENABLE_TERM_ONLY_FEATURE_LIST)
      .action((termOnlyFeatureList, avro2TFParams) => avro2TFParams.copy(termOnlyFeatureList = termOnlyFeatureList))
      .optional()
      .text(
        """Optional.
          |Whether to output term only feature list""".stripMargin
      )

    opt[Boolean](Avro2TFJobParamNames.DISCARD_UNKNOWN_ENTRIES)
      .action(
        (discardUnknownEntries, avro2TFParams) => avro2TFParams
          .copy(discardUnknownEntries = discardUnknownEntries))
      .optional()
      .text(
        """Optional.
          |Whether to discard unknown entries during indices conversion""".stripMargin
      )

    opt[Boolean](Avro2TFJobParamNames.ENABLE_FILTER_ZERO)
      .action(
        (enableFilterZero, avro2TFParams) => avro2TFParams
          .copy(enableFilterZero = enableFilterZero))
      .optional()
      .text(
        """Optional.
          |Whether to enable filter zero for all sparse vector output. Default is false""".stripMargin
      )

    opt[Boolean](Avro2TFJobParamNames.PASS_THROUGH_ONLY)
      .action((passThrough, avro2TFParams) => avro2TFParams.copy(passThroughOnly = passThrough))
      .optional()
      .text(
        """Optional.
          |Whether to pass through inputs and only change outputs formats, num partitions, etc""".stripMargin
      )

    opt[Seq[String]](Avro2TFJobParamNames.FEATURE_LIST_CAP)
      .action {
        (featureListCapStr, avro2TFParams) =>
          val featureListCap = featureListCapStr.map { capStrEntry =>
            val nameAndCap = capStrEntry.split(":")
            require(nameAndCap.size == 2, "Please specify both feature name and its cap, e.g. f1:32,f2:23")
            nameAndCap.head -> nameAndCap.last.toInt
          }.toMap
          avro2TFParams.copy(featureListCap = featureListCap)
      }
      .optional()
      .text(
        """Optional.
          |A list of comma separated feature cap string of feature name to its max feature list size.
          |The feature entries with top frequencies will be kept.
          |Each feature cap string has the format of featureName:capSize""".stripMargin
      )
  }

  /**
   * Parse the Avro2TF job parameters
   *
   * @param args Avro2TF command line arguments
   * @return Avro2TF parameters
   */
  def parse(args: Seq[String]): Avro2TFParams = {

    parser.parse(
      args,
      Avro2TFParams(
        inputPaths = Seq.empty,
        workingDir = null,
        inputDateRange = Seq.empty,
        inputDaysRange = Seq.empty,
        numOfOutputFiles = -1,
        minParts = -1,
        enableShuffle = false,
        externalFeaturesListPath = "",
        avro2TFConfig = null,
        executionMode = TrainingMode.training,
        enableCache = false,
        skipConversion = false,
        outputFormat = AVRO_RECORD,
        extraColumnsToKeep = Seq.empty,
        tensorsSharingFeatureLists = Array[Array[String]](),
        numPartitions = 100,
        partitionFieldName = "",
        termOnlyFeatureList = false,
        discardUnknownEntries = false,
        enableFilterZero = false,
        passThroughOnly = false,
        featureListCap = Map.empty[String, Int]
      )
    ) match {
      case Some(params) =>
        // Check if users only specify either date range or days range
        if (params.inputDateRange.nonEmpty && params.inputDaysRange.nonEmpty) {
          throw new IllegalArgumentException("Please only specify either date range or days range.")
        } else {
          params
        }
      case None => throw new IllegalArgumentException(
        s"Parsing the Avro2TF command line arguments failed.\n" + s"(${args.mkString(", ")}),\n ${parser.usage}"
      )
    }
  }
}
