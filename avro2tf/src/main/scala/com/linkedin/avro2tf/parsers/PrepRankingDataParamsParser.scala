package com.linkedin.avro2tf.parsers

import com.linkedin.avro2tf.constants.PrepRankingJobParamNames
import com.linkedin.avro2tf.utils.TrainingMode

case class PrepRankingDataParams(
  inputDataPath: String,
  inputMetadataPath: String,
  outputDataPath: String,
  outputMetadataPath: String,
  groupIdList: Seq[String],
  groupListMaxSize: Int,
  dropColumns: Option[Seq[String]] = None,
  executionMode: TrainingMode.TrainingMode = TrainingMode.training,
  numOutputFiles: Int = -1,
  enableShuffle: Boolean = false,
  labelPaddingValue: Double = -1.0d,
  featurePaddingValue: Double = 0.0d,
  skipPadding: Boolean = false
)

object PrepRankingDataParamsParser {

  private val parser = new scopt.OptionParser[PrepRankingDataParams](
    "Parsing command line for PrepRankingData job."
  ) {
    // Parse the path to working directory where the output should be saved
    opt[String](PrepRankingJobParamNames.INPUT_DATA_PATH)
      .action((x, p) => p.copy(inputDataPath = x.trim))
      .required()
      .text(
        """Required.
          |The input data path.""".stripMargin
      )

    opt[String](PrepRankingJobParamNames.INPUT_METADATA_PATH)
      .action((x, p) => p.copy(inputMetadataPath = x.trim))
      .required()
      .text(
        """Required.
          |The input metadata path.""".stripMargin
      )

    opt[String](PrepRankingJobParamNames.OUTPUT_DATA_PATH)
      .action((x, p) => p.copy(outputDataPath = x.trim))
      .required()
      .text(
        """Required.
          |The output data path.""".stripMargin
      )

    opt[String](PrepRankingJobParamNames.OUTPUT_METADATA_PATH)
      .action((x, p) => p.copy(outputMetadataPath = x.trim))
      .required()
      .text(
        """Required.
          |The output metadata path.""".stripMargin
      )

    opt[String](PrepRankingJobParamNames.GROUP_ID_LIST)
      .action((x, p) => p.copy(groupIdList = x.split(",").map(_.trim)))
      .required()
      .text(
        """Required.
          |The group id string expression. Supported composite key grouping by separated by comma.
          |For example: groupId1,groupId2
        """.stripMargin)

    opt[Int](PrepRankingJobParamNames.GROUP_LIST_MAX_SIZE)
      .action((x, p) => p.copy(groupListMaxSize = x))
      .required()
      .text(
        """Required.
          |The maximum list length for each query.
        """.stripMargin
      )

    opt[String](PrepRankingJobParamNames.DROP_COLUMN_LIST)
      .action{ (x, p) =>
        val fList = x.split(",").map(_.trim)
        if (fList.isEmpty) {
          p.copy(dropColumns = None)
        } else {
          p.copy(dropColumns = Some(fList))
        }
      }
      .optional()
      .text(
        """Optional.
          |The drop column list separated by comma.
        """.stripMargin
      )

    opt[Int](PrepRankingJobParamNames.NUM_OUTPUT_FILES)
      .action((x, p) => p.copy(numOutputFiles = x))
      .optional()
      .text(
        """Optional.
          |The number of output files with the default number set to -1.
        """.stripMargin
      )

    opt[Boolean](PrepRankingJobParamNames.SHUFFLE)
      .action((x, p) => p.copy(enableShuffle = x))
      .optional()
      .text(
        """Optional.
          |Whether to shuffle the converted training data with the default set to true.""".stripMargin
      )

    // Parse the execution mode, which decides whether to prepare training, validation, or test data
    opt[String](PrepRankingJobParamNames.EXECUTION_MODE)
      .action(
        (x, p) =>
          p.copy(executionMode = TrainingMode.withName(x.toLowerCase))
      )
      .optional()
      .text(
        """Optional.
          |Whether to prepare training, validation, or test data.""".stripMargin
      )

    opt[Double](PrepRankingJobParamNames.LABEL_PADDING_VALUE)
      .action((x, p) => p.copy(labelPaddingValue = x))
      .optional()
      .text(
        """Optional.
          |Padding values for label. Default is -1.0d.
        """.stripMargin
      )

    opt[Double](PrepRankingJobParamNames.FEATURE_PADDING_VALUE)
      .action((x, p) => p.copy(featurePaddingValue = x))
      .optional()
      .text(
        """Optional.
          |Padding values for scalar features. Default is 0.0d.
          |Not applicable for name-term-value features.
        """.stripMargin
      )

    opt[Boolean](PrepRankingJobParamNames.SKIP_PADDING)
      .action((x, p) => p.copy(skipPadding = x))
      .optional()
      .text(
        """Optional.
          |Skip padding option if it's set as true.
          |If padding is skipped, padding is skipped for disk output and user will need to pad it in memory after loading it back
          |The default value is false, which means padding is by default enabled
        """.stripMargin
      )
  }

  def parse(args: Seq[String]): PrepRankingDataParams = {
    parser.parse(
      args,
      PrepRankingDataParams(
        inputDataPath = "",
        inputMetadataPath = "",
        outputDataPath = "",
        outputMetadataPath = "",
        groupIdList = Seq.empty,
        groupListMaxSize = 0
      )
    ) match {
      case Some(params) => params
      case None => throw new IllegalArgumentException(
        s"Parsing the command line failed.\n${args.mkString(", ")}\n${parser.usage}"
      )
    }
  }
}
