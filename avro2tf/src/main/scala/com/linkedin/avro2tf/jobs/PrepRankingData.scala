package com.linkedin.avro2tf.jobs

import scala.collection.mutable
import com.databricks.spark.avro._
import com.linkedin.avro2tf.configs.TensorMetadata
import com.linkedin.avro2tf.helpers.TensorizeInJobHelper
import com.linkedin.avro2tf.jobs.TensorizeIn.SparseVector
import com.linkedin.avro2tf.parsers.{PrepRankingDataParams, PrepRankingDataParamsParser}
import com.linkedin.avro2tf.utils._
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType, StructType, DataType => ADataType}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.parser.{decode => jsondecode}

/**
 * Prepare ranking tensor data after TensorizeIn job.
 * The output data should be ready be consumed by TensorflowIn and tf.ranking
 */
object PrepRankingData {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  val DefaultRankingId = "DefaultRankingId"

  /**
   * The main function to perform PrepRankingData job
   *
   * @param spark The spark session
   * @param params The prepare ranking data parameters specified by user
   */
  def run(spark: SparkSession, params: PrepRankingDataParams): Unit = {

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val metadata = readMetadata(fs, params.workingDir.tensorMetadataPath)

    require(metadata.contains(Constants.FEATURES), "Cannot find features section in metadata.")
    require(
      metadata.contains(Constants.LABELS) && metadata(Constants.LABELS).size == 1,
      "Cannot find labels section in metadata or length is not equal to 1.")

    val inputPath = params.executionMode match {
      case TrainingMode.training => params.workingDir.trainingDataPath
      case TrainingMode.validation => params.workingDir.validationDataPath
      case TrainingMode.test => params.workingDir.testDataPath
      case _ => throw new IllegalArgumentException("Unknown training mode!")
    }

    require(fs.exists(new Path(inputPath)), s"Cannot find $inputPath on hdfs.")
    prepareRanking(spark, inputPath, params, metadata)
  }

  /**
   * Reading tensor_metadata.json from the TensorizeIn job
   *
   * @param fs The file system
   * @param path The tensor_metadata.json file path string
   * @return The map of tensor metadata for labels and features
   */
  private def readMetadata(fs: FileSystem, path: String): Map[String, Seq[TensorMetadata]] = {
    import com.linkedin.avro2tf.configs.JsonCodecs._

    val metaDataPath = new Path(path)
    require(fs.exists(metaDataPath), "Cannot find the tensor_metadata.json file!")
    val metaDataJson = IOUtils.readContentFromHDFS(fs, metaDataPath)

    jsondecode[Map[String, Seq[TensorMetadata]]](metaDataJson) match {
      case Left(e) => throw e
      case Right(m) => m
    }
  }

  /**
   * Prepare ranking data based on the user configuration
   *
   * @param spark The spark session
   * @param inputPath The input data path
   * @param params The user input parameter
   * @param metadata The metadata from TensorizeIn job
   */
  def prepareRanking(
    spark: SparkSession,
    inputPath: String,
    params: PrepRankingDataParams,
    metadata: Map[String, Seq[TensorMetadata]]): Unit = {

    // read data
    val df = IOUtils.readAvro(spark, inputPath).drop(params.dropColumns.getOrElse(Seq.empty): _*)

    // three types of columns: qid column, content features columns, query feature columns
    val groupIdCol = Seq(col(params.groupId).alias(DefaultRankingId))
    val queryFeatures = params.queryFeatureList.getOrElse(Seq.empty)
    val contentFeatures = (params.contentFeatureList match {
      case Some(x) => x
      case None => df.columns.toSeq
    }).filter(x => x != params.groupId && !queryFeatures.contains(x))
    val label = metadata(Constants.LABELS).head.name

    val selectedColumnNames = (contentFeatures ++ queryFeatures).distinct.map(col)
    val selectDf = df.select(groupIdCol ++ selectedColumnNames: _*)

    // group by qid
    // content features - collect_list
    // query features - first row
    val aggExpr = contentFeatures.map(x => collect_list(col(x)).alias(x)) ++
      queryFeatures.map(x => first(col(x)).alias(x))
    val groupDf = selectDf
      .groupBy(DefaultRankingId)
      .agg(aggExpr.head, aggExpr.tail: _*)

    // truncate the list of each group to user specified size
    // TODO: padding of the features if each group has different length
    // For sparse tensor variable length is acceptable but for dense tensor padding is needed.
    var truncateDf = groupDf
    contentFeatures.foreach { it =>
      val isLabel = it.equals(label)
      truncateDf = truncateDf
        .withColumn(
          it,
          takeFirstN(
            selectDf.schema(it).dataType,
            params.groupListMaxSize,
            isLabel,
            params.labelPaddingValue,
            params.featurePaddingValue)(col(it)))
    }

    // flatten indices of sparse tensors
    val contentFeaturesWithSpVec = contentFeatures.filter(
      it =>
        CommonUtils.isArrayOfSparseTensor(truncateDf.schema(it).dataType))
    var transformDf = truncateDf.drop(DefaultRankingId)

    contentFeaturesWithSpVec.foreach { it =>
      transformDf = transformDf.withColumn(it, flattenSparseVectorArray(col(it)))
    }

    // update tensor_metadata.json
    val outputPath = params.executionMode match {
      case TrainingMode.training =>
        updateMetadata(spark, params, contentFeatures, transformDf, metadata)
        params.workingDir.rankingTrainingPath
      case TrainingMode.validation => params.workingDir.rankingValidationPath
      case TrainingMode.test => params.workingDir.rankingTestPath
      case _ => throw new IllegalArgumentException("Unknown training mode!")
    }

    // write to disk
    val repartitionDf = TensorizeInJobHelper
      .repartitionData(transformDf, params.numOutputFiles, params.enableShuffle)
    repartitionDf.write.mode(SaveMode.Overwrite).avro(outputPath)
  }

  /**
   * Update metadata information after grouping
   *
   * @param spark The spark session
   * @param params The job params
   * @param contentFeatures The list of content features
   * @param transformDf The data frame after grouping by qid
   * @param metadata The metadata
   */
  private def updateMetadata(
    spark: SparkSession,
    params: PrepRankingDataParams,
    contentFeatures: Seq[String],
    transformDf: DataFrame,
    metadata: Map[String, Seq[TensorMetadata]]
  ): Unit = {
    import com.linkedin.avro2tf.configs.JsonCodecs._

    // update shape information in tensor_metadata
    val updateMetadata = metadata.mapValues { tensorMetaDatas =>
      tensorMetaDatas.filter(_.name != params.groupId).map { x =>
        if (contentFeatures.contains(x.name)) {
          x.copy(shape = Array(params.groupListMaxSize) ++ x.shape)
        } else {
          x
        }
      }
    }
    val updateMetadataJson = updateMetadata.asJson.toString
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    IOUtils
      .writeContentToHDFS(
        fs,
        new Path(params.workingDir.rankingTensorMetadataPath),
        updateMetadataJson,
        overwrite = true)

    // write content feature list
    IOUtils.writeContentToHDFS(
      fs,
      new Path(params.workingDir.rankingContentFeatureList),
      contentFeatures.mkString("\n"),
      overwrite = true
    )
  }

  /**
   * udf to flatten the 2d sparse tensor to 1d.
   */
  private val flattenSparseVectorArray =
    udf(
      (arrayOfSp: Seq[Row]) => {

        val indices = mutable.ArrayBuffer[Long]()
        val values = mutable.ArrayBuffer[Float]()
        arrayOfSp.zipWithIndex.map { case (sp, index) =>
          indices ++= sp.getAs[Seq[Long]](Constants.INDICES).flatMap(x => Seq(index, x))
          values ++= sp.getAs[Seq[Float]](Constants.VALUES)
        }
        SparseVector(indices, values)
      })

  /**
   * From data schema to get correct udf
   *
   * @param dtype The input data schema
   * @param groupListMaxSize The number of records taken from array
   * @param isLabel Whether the column is a label field
   * @param labelPaddingValue The label padding value
   * @param featurePaddingValue The feature padding value
   * @return The udf does the taking first n items from array in a column
   */
  private def takeFirstN(
    dtype: ADataType,
    groupListMaxSize: Int,
    isLabel: Boolean = false,
    labelPaddingValue: Double = -1.0d,
    featurePaddingValue: Double = 0.0d) = {

    def takeFunc[T](paddingValue: T) = (array: Seq[T]) => {
      val filledArray = if (array.size >= groupListMaxSize) {
        array
      } else {
        array ++ Seq.fill(groupListMaxSize - array.size)(paddingValue)
      }
      filledArray.take(groupListMaxSize)
    }

    def takeSpFunc = (array: Seq[SparseVector]) => array.take(groupListMaxSize)

    val padValue = if (isLabel) {
      labelPaddingValue
    } else {
      featurePaddingValue
    }

    dtype match {
      case _: FloatType => udf(takeFunc[Float](padValue.toFloat))
      case _: DoubleType => udf(takeFunc[Double](padValue.toDouble))
      case _: IntegerType => udf(takeFunc[Int](padValue.toInt))
      case _: LongType => udf(takeFunc[Long](padValue.toLong))
      case _: StructType => udf(takeSpFunc)
      case _ => throw new UnsupportedOperationException(s"Cannot support type: $dtype")
    }
  }

  /**
   * Main logic
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val params = PrepRankingDataParamsParser.parse(args)
    val ss = SparkSession.builder().appName(getClass.getName).getOrCreate()

    try {
      run(ss, params)
    } finally {
      ss.stop()
    }
  }
}
