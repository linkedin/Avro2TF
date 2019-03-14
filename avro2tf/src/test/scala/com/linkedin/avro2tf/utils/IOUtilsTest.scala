package com.linkedin.avro2tf.utils

import java.time.{LocalDate, LocalDateTime}

import org.apache.spark.sql.SparkSession
import org.testng.Assert._
import org.testng.annotations.Test

class IOUtilsTest extends WithLocalSparkSession with PrepareData {

  /**
   * Test if non-existing daily folders can be filtered out
   */
  @Test
  def testGetPathList(): Unit = {
    // we specify 4 days, but we only have path for 3 days in file system
    val startLocalDate: LocalDateTime = LocalDate.now().atStartOfDay().minusDays(4)
    val endLocalDate: LocalDateTime = LocalDate.now().atStartOfDay()
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    val list = IOUtils.getPathList(spark, rootPath, startLocalDate, endLocalDate)
    assertEquals(list, dailyPathList)
  }

  /**
   * Test readAvro method, when date range is specified
   */
  @Test
  def testReadAvroWithDates(): Unit = {
    val startDate = LocalDate.now().minusDays(3).format(IOUtils.dateStampFormatter)
    val endDate = LocalDate.now().minusDays(1).format(IOUtils.dateStampFormatter)
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    val df  = IOUtils.readAvro(spark, rootPath, startDate, endDate)

    // verify the correct number records are read into dataframe
    assertEquals(df.count(), 9)
  }

  /**
   * Test readAvro method, when date offset is specified
   */
  @Test
  def testReadAvroWithOffsets(): Unit = {
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    val df = IOUtils.readAvro(spark, rootPath, 3, 1)

    // verify the correct number records are read into dataframe
    assertEquals(df.count(), 9)
  }

  /**
   * Test readAvro method, when date start date offset and end date are specified
   */
  @Test
  def testReadAvroWithMix(): Unit = {
    val spark = SparkSession.builder().appName(getClass.getName).getOrCreate()
    val endDate = LocalDate.now().minusDays(1).format(IOUtils.dateStampFormatter)
    val df = IOUtils.readAvro(spark, rootPath, endDate, 3)
    // verify the correct number records are read into dataframe
    assertEquals(df.count(), 9)
  }
}
