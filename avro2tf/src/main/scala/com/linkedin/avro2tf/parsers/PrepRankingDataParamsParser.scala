package com.linkedin.avro2tf.parsers

import com.linkedin.avro2tf.utils.TrainingMode

case class PrepRankingDataParams(
  workingDir: WorkingDirectory,
  groupId: String,
  groupListMaxSize: Int,
  contentFeatureList: Option[Seq[String]],
  queryFeatureList: Option[Seq[String]],
  enableFilterZero: Boolean = false,
  dropColumns: Option[Seq[String]] = None,
  executionMode: TrainingMode.TrainingMode = TrainingMode.training,
  numOutputFiles: Int = -1,
  enableShuffle: Boolean = false,
  labelPaddingValue: Double = -1.0d,
  featurePaddingValue: Double = 0.0d
)

object PrepRankingDataParamsParser {

  private val parser = new scopt.OptionParser[PrepRankingDataParams](
    "Parsing command line for PrepRankingData job."
  ) {
    // Parse the path to working directory where the output should be saved
    opt[String]("working-dir")
      .action((x, p) => p.copy(workingDir = WorkingDirectory(x.trim)))
      .required()
      .text(
        """Required.
          |The path to working directory where the output should be saved.""".stripMargin
      )

    opt[String]("group-id")
      .action((x, p) => p.copy(groupId = x.trim))
      .required()
      .text(
        """Required
          |The group id string expression.
        """.stripMargin)

    opt[Int]("group-list-max-size")
      .action((x, p) => p.copy(groupListMaxSize = x))
      .required()
      .text(
        """Optional.
          |The maximum list length for each query.
        """.stripMargin
      )

    opt[String]("content-feature-list")
      .action{ (x, p) =>
        val fList = x.split(",").map(_.trim)
        if (fList.isEmpty) {
          p.copy(contentFeatureList = None)
        } else {
          p.copy(contentFeatureList = Some(fList))
        }
      }
      .optional()
      .text(
        """Optional.
          |The content feature list separated by comma. If not provided, all the features are considered
          |as content features.
        """.stripMargin
      )

    opt[String]("query-feature-list")
      .action{ (x, p) =>
        val fList = x.split(",").map(_.trim)
        if (fList.isEmpty) {
          p.copy(queryFeatureList = None)
        } else {
          p.copy(queryFeatureList = Some(fList))
        }
      }
      .optional()
      .text(
        """Optional.
          |The query feature list separated by comma.
        """.stripMargin
      )

    opt[Boolean]("enable-filter-zero")
      .action( (x, p) => p.copy(enableFilterZero = x))
      .optional()
      .text(
        """Optional
          |Whether to filter out the zero values in features. Default is false.
        """.stripMargin
      )

    opt[String]("drop-column-list")
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

    opt[Int]("num-output-files")
      .action((x, p) => p.copy(numOutputFiles = x))
      .optional()
      .text(
        """Optional.
          |The number of output files with the default number set to -1.
        """.stripMargin
      )

    opt[Boolean]("shuffle")
      .action((x, p) => p.copy(enableShuffle = x))
      .optional()
      .text(
        """Optional.
          |Whether to shuffle the converted training data with the default set to true.""".stripMargin
      )

    // Parse the execution mode, which decides whether to prepare training, validation, or test data
    opt[String]("execution-mode")
      .action(
        (x, p) =>
          p.copy(executionMode = TrainingMode.withName(x.toLowerCase))
      )
      .optional()
      .text(
        """Optional.
          |Whether to prepare training, validation, or test data.""".stripMargin
      )

    opt[Double]("label-padding-value")
      .action((x, p) => p.copy(labelPaddingValue = x))
      .optional()
      .text(
        """Optional.
          |Padding values for label. Default is -1.0d.
        """.stripMargin
      )

    opt[Double]("feature-padding-value")
      .action((x, p) => p.copy(featurePaddingValue = x))
      .optional()
      .text(
        """Optional.
          |Padding values for scalar features. Default is 0.0d.
          |Not applicable for name-term-value features.
        """.stripMargin
      )
  }

  def parse(args: Seq[String]): PrepRankingDataParams = {
    parser.parse(
      args,
      PrepRankingDataParams(
        workingDir = null,
        groupId = "",
        groupListMaxSize = 0,
        contentFeatureList = None,
        queryFeatureList = None
      )
    ) match {
      case Some(params) => params
      case None => throw new IllegalArgumentException(
        s"Parsing the command line failed.\n${args.mkString(", ")}\n${parser.usage}"
      )
    }
  }
}
