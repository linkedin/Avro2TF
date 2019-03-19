package com.linkedin.avro2tf.parsers

import scala.io.Source

import com.linkedin.avro2tf.configs.TensorizeInConfiguration
import com.linkedin.avro2tf.utils.Constants._

/**
 * TensorizeIn parsed parameters will be put into this case class for ease of access
 *
 * @param inputPaths A list of input paths
 * @param workingDir All intermediate and final results will be written to this directory
 * @param inputDateRange Date range for daily structured input path
 * @param inputDaysRange Days offset range for daily structured input path
 * @param numOfOutputFiles The number of files written to HDFS
 * @param minParts Minimum number of partitions for input data
 * @param enableShuffle Whether to enable shuffling the final output data
 * @param externalFeaturesListPath Path of external feature list supplied by the user
 * @param tensorizeInConfig TensorizeIn configuration for features and labels to be tensorized
 * @param isTrainMode Whether preparing training data or test data
 * @param executionMode One of "train", "validate" or "test"
 * @param enableCache Whether to enable caching the intermediate Spark DataFrame result
 * @param skipConversion Indicate whether to skip the conversion step
 * @param outputFormat Output format of tensorized data, e.g. Avro or TFRecord
 */
case class TensorizeInParams(
  inputPaths: Seq[String],
  workingDir: WorkingDirectory,
  inputDateRange: Seq[String],
  inputDaysRange: Seq[Int],
  numOfOutputFiles: Int,
  minParts: Int,
  enableShuffle: Boolean,
  externalFeaturesListPath: String,
  tensorizeInConfig: TensorizeInConfiguration,
  isTrainMode: Boolean,
  executionMode: String,
  enableCache: Boolean,
  skipConversion: Boolean,
  outputFormat: String,
  extraColumnsToKeep: Seq[String]
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
  val schemaFilePath = s"$rootPath/$SCHEMA_FILE_NAME"
  val tensorMetadataPath = s"$rootPath/$METADATA_DIR_NAME/$TENSOR_METADATA_FILE_NAME"
}

/**
 * Parser file for TensorizeIn job parameters from command line arguments
 */
object TensorizeInJobParamsParser {

  /**
   * Parser to parse TensorizeIn job parameters
   */
  private val parser = new scopt.OptionParser[TensorizeInParams](
    "Parsing command line for TensorizeIn job") {

    // Parse a list of comma separated paths for input
    opt[Seq[String]]("input-paths")
      .action((inputPaths, tensorizeInParams) => tensorizeInParams.copy(inputPaths = inputPaths))
      .required()
      .text(
        """Required.
          |A list of comma separated paths for input.""".stripMargin
      )

    // Parse the path to working directory where the output should be saved
    opt[String]("working-dir")
      .action((workingDir, tensorizeInParams) => tensorizeInParams.copy(workingDir = WorkingDirectory(workingDir.trim)))
      .required()
      .text(
        """Required.
          |The path to working directory where the output should be saved.""".stripMargin
      )

    // Parse the input date range in the format of yyyymmdd-yyyymmdd
    opt[String]("input-date-range")
      .action(
        (intputDateRange, tensorizeInParams) => {
          val dates = intputDateRange.trim.split('-')
          require(dates.length == 2, "must have start and end date")
          tensorizeInParams.copy(inputDateRange = dates.map(_.trim))
        }
      )
      .text(
        """Optional.
          |The input date range in the format of yyyymmdd-yyyymmdd.""".stripMargin
      )

    // Parse the input days range in the format of startOffest-endOffset
    opt[String]("input-days-range")
      .action(
        (inputDaysRange, tensorizeInParams) => {
          val daysOffset = inputDaysRange.trim.split('-')
          require(daysOffset.length == 2, "must have start and end days offset")
          val intDaysOffset = daysOffset.map(_.trim.toInt)
          require(intDaysOffset.forall(_ >= 0), s"days-range can not be negative value: $inputDaysRange")
          tensorizeInParams.copy(inputDaysRange = intDaysOffset)
        }
      )
      .text(
        """Optional.
          |The input days range in the format of startOffest-endOffset.""".stripMargin
      )

    // Parse the number of output files with the default set to -1
    opt[Int]("num-output-files")
      .action((numOfOutputFiles, tensorizeInParams) => tensorizeInParams.copy(numOfOutputFiles = numOfOutputFiles))
      .optional()
      .text(
        """Optional.
          |The number of output files with the default set to -1.""".stripMargin
      )

    // Parse the minimum number of partitions for input data; if below this threshold, repartition will be triggered
    opt[Int]("min-parts")
      .action(
        (minParts, tensorizeInParams) => {
          require(minParts > 0, "min-parts must be greater than 0")
          tensorizeInParams.copy(minParts = minParts)
        }
      )
      .optional()
      .text(
        """Optional.
          |The minimum number of partitions for input data; if below this threshold, repartition will be triggered."""
          .stripMargin
      )

    // Parse whether to shuffle the converted training data with the default set to true
    opt[Boolean]("shuffle")
      .action((enableShuffle, tensorizeInParams) => tensorizeInParams.copy(enableShuffle = enableShuffle))
      .optional()
      .text(
        """Optional.
          |Whether to shuffle the converted training data with the default set to true.""".stripMargin
      )

    // Parse the path to external feature list where the user supplied feature metadata is written
    opt[String]("external-feature-list-path")
      .action((externalFeatureListPath, tensorizeInParams) => tensorizeInParams.copy(externalFeaturesListPath = externalFeatureListPath.trim))
      .text(
        """Optional.
          |The path to external feature list where the user supplied feature metadata is written.""".stripMargin
      )

    // Parse the TensorizeIn configuration in JSON format
    opt[String]("tensorizeIn-config-path")
      .action(
        (tensorizeInConfigPath, tensorizeInParams) => {

          val bufferedSource = Source.fromFile(tensorizeInConfigPath)
          val jsonInput = bufferedSource.mkString
          bufferedSource.close()

          // Get the TensorizeIn Configuration
          val tensorizeInConfiguration = TensorizeInConfigParser.getTensorizeInConfiguration(jsonInput)

          tensorizeInParams.copy(tensorizeInConfig = tensorizeInConfiguration)
        }
      )
      .text(
        """Required.
          |The TensorizeIn configuration in JSON format.""".stripMargin
      )

    // Parse whether to prepare training data or test data
    opt[Boolean]("train-mode")
      .action((isTrainMode, tensorizeInParams) => tensorizeInParams.copy(isTrainMode = isTrainMode))
      .optional()
      .text(
        """Optional (deprecated please use execution-mode).
          |Whether to prepare training data or test data.""".stripMargin
      )

    // Parse the execution mode, which decides whether to prepare training, validation, or test data
    opt[String]("execution-mode")
      .action(
        (executionMode, tensorizeInParams) => {
          if (Array(TRAINING_EXECUTION_MODE, VALIDATION_EXECUTION_MODE, TEST_EXECUTION_MODE).contains(executionMode.toLowerCase)) {
            tensorizeInParams.copy(executionMode = executionMode.toLowerCase)
          } else {
            throw new IllegalArgumentException("Execution mode must be one of 'train', 'validate', or 'test'.")
          }
        }
      )
      .optional()
      .text(
        """Optional.
          |Whether to prepare training, validation, or test data.""".stripMargin
      )

    // Parse whether to cache the intermediate Spark DataFrame result with default set to false
    opt[Boolean]("enable-cache")
      .action((enableCache, tensorizeInParams) => tensorizeInParams.copy(enableCache = enableCache))
      .optional()
      .text(
        """Optional.
          |Whether to cache the intermediate Spark DataFrame result with default set to false.""".stripMargin
      )

    // Parse whether to skip the conversion step with default set to false
    opt[Boolean]("skip-conversion")
      .action((skipConversion, tensorizeInParams) => tensorizeInParams.copy(skipConversion = skipConversion))
      .optional()
      .text(
        """Optional.
          |Whether to skip the conversion step with default set to false.""".stripMargin
      )

    // Parse the output format of tensorized data, e.g. Avro or TFRecord
    opt[String]("output-format")
      .action((outputFormat, tensorizeInParams) => tensorizeInParams.copy(outputFormat = outputFormat))
      .optional()
      .text(
        """Optional.
          |The output format of tensorized data, e.g. Avro or TFRecord.""".stripMargin
      )

    opt[Seq[String]]("extra-columns-to-keep")
      .action((extraColumns, tensorizeInParams) => tensorizeInParams.copy(extraColumnsToKeep = extraColumns))
      .optional()
      .text(
        """Optional.
          |A list of comma separated column names to specify extra columns to keep.""".stripMargin
      )
  }

  /**
   * Parse the TensorizeIn job parameters
   *
   * @param args TensorizeIn command line arguments
   * @return TensorizeIn parameters
   */
  def parse(args: Seq[String]): TensorizeInParams = {

    parser.parse(
      args,
      TensorizeInParams(
        inputPaths = Seq.empty,
        workingDir = null,
        inputDateRange = Seq.empty,
        inputDaysRange = Seq.empty,
        numOfOutputFiles = -1,
        minParts = -1,
        enableShuffle = false,
        externalFeaturesListPath = "",
        tensorizeInConfig = null,
        isTrainMode = true,
        executionMode = TRAINING_EXECUTION_MODE,
        enableCache = false,
        skipConversion = false,
        outputFormat = AVRO_RECORD,
        extraColumnsToKeep = Seq.empty
      )
    ) match {
      case Some(params) =>
        // Check if users only specify either date range or days range
        if (params.inputDateRange.nonEmpty && params.inputDaysRange.nonEmpty) {
          throw new IllegalArgumentException("Please only specify either date range or days range.")
        }
        if(!params.isTrainMode && params.executionMode == TRAINING_EXECUTION_MODE){
          params.copy(executionMode = TEST_EXECUTION_MODE)
        } else {
          params
        }
      case None => throw new IllegalArgumentException(
        s"Parsing the TensorizeIn command line arguments failed.\n" + s"(${args.mkString(", ")}),\n ${parser.usage}")
    }
  }
}