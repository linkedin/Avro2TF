package com.linkedin.avro2tf.utils

import java.io.{BufferedWriter, File, OutputStreamWriter, PrintWriter}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}

import com.databricks.spark.avro.SchemaConverters
import com.linkedin.avro2tf.utils.Constants._
import org.apache.avro.SchemaBuilder
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object IOUtils {
  /**
   * Spark format to read Avro files.
   */
  private val databricksAvroFormat = "com.databricks.spark.avro"

  val dateStampFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  val logger: Logger = LoggerFactory.getLogger(IOUtils.getClass)

  /**
   * Creates a File Path given one or multiple parts and returns it normalized
   * (no multiple consecutive or trailing "/")
   * @param pathParts  A Varargs with the parts used to create the String path
   * @return A normalized (no multiple consecutive or trailing "/") String path
   *         given for the provided path parts
   */
  def createStringPath(pathParts: String*): String = {
    new File(pathParts.map(_.trim).mkString("/")).toString
  }

  /**
   * An object for defining the date/hour formats in the dated directories.
   * This object is used by getPaths to format the returned paths.
   * An example of how this object can be overridden to change the format exists in the test.
   */
  object TemporalPathFormats {
    trait TemporalPathFormat[T <: ChronoUnit] {
      val formatter: DateTimeFormatter
    }
    implicit object Daily extends TemporalPathFormat[ChronoUnit.DAYS.type] {
      override val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    }
    implicit object Hourly extends TemporalPathFormat[ChronoUnit.HOURS.type] {
      override val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH")
    }
  }

  /**
   * Returns a Seq of paths that are prefixed with basePath, and that cover the
   * period [startInclusive, endExclusive). The dates are formatted using
   * TemporalPathFormats or any object similarly defined.
   * @param basePath The prefix path under which the dated directories exist.
   * @param startInclusive The start time (e.g., 2017/01/01). Included in the returned paths.
   * @param endExclusive The end time (e.g., 2017/01/03). Not included in the returned paths.
   * @param unit The time unit (e.g., ChronoUnit.DAYS).
   * @tparam T The type of the unit (e.g., ChronoUnit.DAYS.type)
   * @return A Seq of paths under basePath covering the period [startInclusive, endExclusive).
   */
  def getPaths[T <: ChronoUnit: TemporalPathFormats.TemporalPathFormat](
    basePath: String,
    startInclusive: LocalDateTime,
    endExclusive: LocalDateTime,
    unit: ChronoUnit): Seq[String] = {
    getPaths(basePath, startInclusive, endExclusive, unit,
      implicitly[TemporalPathFormats.TemporalPathFormat[T]].formatter)
  }

  /**
   * Returns a Seq of paths that are prefixed with basePath, and that cover the
   * period [startInclusive, endExclusive). The dates are formatted using formatter.
   * @param basePath The prefix path under which the dated directories exist.
   * @param startInclusive The start time (e.g., 2017/01/01). Included in the returned paths.
   * @param endExclusive The end time (e.g., 2017/01/03). Not included in the returned paths.
   * @param unit The time unit (e.g., ChronoUnit.DAYS).
   * @param formatter A DateTimeFormatter for formatting the dates in the paths.
   * @return A Seq of paths under basePath covering the period [startInclusive, endExclusive).
   */
  def getPaths(
    basePath: String,
    startInclusive: LocalDateTime,
    endExclusive: LocalDateTime,
    unit: ChronoUnit,
    formatter: DateTimeFormatter): Seq[String] = {
    getTemporalRange(startInclusive, endExclusive, unit)
      .map { time => createStringPath(basePath, formatter.format(time)) }
  }

  /**
   * Returns a Seq of LocalDateTime that cover the period [startInclusive, endExclusive).
   * Throws an IllegalArgumentException if startInclusive or endExclusive are not at
   * the boundary of unit, or if startInclusive >= endExclusive.
   * @param startInclusive The start time (e.g., 2017/01/01). Included in the returned paths.
   * @param endExclusive The end time (e.g., 2017/01/03). Not included in the returned paths.
   * @param unit The time unit (e.g., ChronoUnit.DAYS).
   * @return A Seq of LocalDateTime covering the period [startInclusive, endExclusive).
   */
  private def getTemporalRange(
    startInclusive: LocalDateTime,
    endExclusive: LocalDateTime,
    unit: ChronoUnit): Seq[LocalDateTime] = {
    if (startInclusive.truncatedTo(unit) != startInclusive) {
      throw new IllegalArgumentException(
        s"Invalid argument: The startInclusive ($startInclusive}) " +
          s"should be at the boundary of unit ($unit}).")
    }
    if (endExclusive.truncatedTo(unit) != endExclusive) {
      throw new IllegalArgumentException(
        s"Invalid argument: The endExclusive ($endExclusive}) " +
          s"should be at the boundary of unit ($unit}).")
    }
    if (!endExclusive.isAfter(startInclusive)) {
      throw new IllegalArgumentException(
        s"Invalid arguments: The startInclusive ($startInclusive}) " +
          s"should be before endExclusive ($endExclusive}).")
    }
    val temporalRange =
      for (step <- 0 until unit.between(startInclusive, endExclusive).toInt)
        yield startInclusive.plus(step, unit)
    if (temporalRange.isEmpty) {
      throw new IllegalArgumentException (
        s"Invalid config: The temporalRange ($temporalRange}) " +
          s"is empty.")
    }
    if (temporalRange.distinct.length != temporalRange.length) {
      throw new IllegalArgumentException(
        s"Invalid config: The temporalRange ($temporalRange}) " +
          s"has duplicate values.")
    }
    temporalRange
  }

  /**
   * Get the list of daily path in the specified range and filter out non-existing ones
   *
   * @param spark SparkSessino
   * @param rootPath The root path of your daily structured dirs
   * @param startDate (inclusive)
   * @param endDate (exclusive)
   * @return
   */
  def getPathList(
    spark: SparkSession,
    rootPath: String,
    startDate: LocalDateTime,
    endDate: LocalDateTime): Seq[String] = {

    val dailyPathList: Seq[String] = IOUtils.getPaths[ChronoUnit.DAYS.type](
      IOUtils.createStringPath(rootPath, "daily"), startDate, endDate, ChronoUnit.DAYS)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val filteredList = dailyPathList.filter {
      p => {
        val isExist = fs.exists(new Path(p))
        if(!isExist) {logger.warn(s"Path: $p does not exist in your specified date range")}
        else {logger.info(s"loading path: $p")}
        isExist
      }
    }
    fs.close()
    filteredList
  }

  /**
   * Read Avro files into a DataFrame given one or multiple input paths
   *
   * @param paths One or more input paths
   * @return DataFrame for the given range of paths
   */
  def readAvro(spark: SparkSession, paths: String*): DataFrame = {

    spark.read.format(databricksAvroFormat).load(paths: _*)
  }

  /**
   *
   * @param spark SparkSession
   * @param rootPath The root path of your daily structured dirs
   * @param startDate String formatted as yyyy-mm-dd (inclusive)
   * @param endDate String formatted as yyyy-mm-dd (inclusive)
   * @return DataFrame for the given range of paths
   */
  def readAvro(spark: SparkSession, rootPath: String, startDate: String, endDate: String): DataFrame = {

    val startLocalDate: LocalDateTime = LocalDate.parse(startDate, dateStampFormatter).atStartOfDay()
    val endLocalDate: LocalDateTime = LocalDate.parse(endDate, dateStampFormatter).atStartOfDay().plusDays(1)
    val filteredPathList = getPathList(spark, rootPath, startLocalDate, endLocalDate)
    readAvro(spark, filteredPathList: _*)
  }

  /**
   *
   * @param spark SparkSession
   * @param rootPath The root path of your daily structured dirs
   * @param startDateOffset The number of days before current day (inclusive)
   * @param endDateOffset The number of days before current day (inclusive)
   * @return DataFrame for the given range of paths
   */
  def readAvro(spark: SparkSession, rootPath: String, startDateOffset: Int, endDateOffset: Int): DataFrame = {

    val startLocalDate: LocalDateTime = LocalDate.now().atStartOfDay().minusDays(startDateOffset)
    val endLocalDate: LocalDateTime = LocalDate.now().atStartOfDay().minusDays(endDateOffset - 1)
    val filteredPathList = getPathList(spark, rootPath, startLocalDate, endLocalDate)
    readAvro(spark, filteredPathList: _*)
  }

  /**
   * Utility to read numDays worth of data ending on endDate (yyyy-mm-dd) (end date inclusive) as a DataFrame
   *
   * @param spark SparkSession
   * @param rootPath The root path of your daily structured dirs
   * @param endDate String formatted as yyyy-mm-dd (inclusive)
   * @param numDays Number of days of data ending at endDate
   * @return DataFrame for the given range of paths
   */
  def readAvro(spark: SparkSession, rootPath: String, endDate: String, numDays: Int): DataFrame = {

    val endLocalDate: LocalDateTime = LocalDate.parse(endDate, dateStampFormatter).atStartOfDay().plusDays(1)
    val startLocalDate: LocalDateTime = endLocalDate.minusDays(numDays)
    val filteredPathList = getPathList(spark, rootPath, startLocalDate, endLocalDate)
    readAvro(spark, filteredPathList: _*)
  }

  /**
   * Save Avro schema of a Spark DataFrame to HDFS
   *
   * @param schema Schema of a Spark DataFrame
   * @param fileSystem Hadoop file system
   * @param path Path to save the Avro schema
   */
  def saveAvroSchemaToHDFS(
    schema: StructType,
    fileSystem: FileSystem,
    path: String): Unit = {

    // Create a builder for an Avro record with the specified name and clear the name space
    val build = SchemaBuilder.record(AVRO_NAME_SPACE).namespace(EMPTY_AVRO_NAME_SPACE)
    // Convert a struct type to an Avro schema with pretty-print JSON
    val avroSchema = SchemaConverters.convertStructToAvro(schema, build, AVRO_NAME_SPACE).toString(ENABLE_PRETTY_PRINT_JSON)

    writeContentToHDFS(fileSystem, new Path(path), avroSchema, overwrite = ENABLE_HDFS_OVERWRITE)
  }

  /**
   * Write the content to a HDFS location
   *
   * @param fileSystem Hadoop file system
   * @param path Output path for the file
   * @param content Content to be written to HDFS
   * @param overwrite Whether to overwrite an existing file
   */
  def writeContentToHDFS(
    fileSystem: FileSystem,
    path: Path,
    content: String,
    overwrite: Boolean): Unit = {

    // Check if output path for the file exists in the file system.
    if (fileSystem.exists(path)) {
      // Delete existing path if overwrite enabled
      if (overwrite) fileSystem.delete(path, ENABLE_RECURSIVE) else return
    }

    val writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(fileSystem.create(path))))

    try {
      writer.write(content)
    } finally {
      writer.close()
    }
  }
}
