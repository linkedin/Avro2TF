package com.linkedin.avro2tf.utils

import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}

import com.databricks.spark.avro._
import com.linkedin.avro2tf.jobs.TensorizeIn.NameTermValue
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.testng.annotations.{AfterClass, BeforeClass}

/**
 * Helps to generate some daily structured data for a test class
 */
trait PrepareData {
  val rootPath = "/tmp/avro2tf-preparedData"
  var dailyPathList: Seq[String] = _

  /**
   * This method generate a list of daily structured paths for test methods to use
   *
   * Three daily path will be generated (three days before current day)
   */
  @BeforeClass
  def prepareData(): Unit = {

    val startDate: LocalDateTime = LocalDate.now().atStartOfDay().minusDays(3)
    val endDate: LocalDateTime = LocalDate.now().atStartOfDay()
    dailyPathList = IOUtils.getPaths[ChronoUnit.DAYS.type](
      IOUtils.createStringPath(rootPath, "daily"), startDate, endDate, ChronoUnit.DAYS)
    val sparkConf: SparkConf = new SparkConf().set("spark.driver.bindAddress", "127.0.0.1")
    val spark = SparkSession.builder().master("local[*]").appName("prepare-data").config(sparkConf).getOrCreate()
    import spark.implicits._
    dailyPathList.foreach { path =>
      Seq(
        (1, Seq(NameTermValue("f1", "1", 1), NameTermValue("f1", "2", 1), NameTermValue("f2", "a", 1))),
        (0, Seq(NameTermValue("f1", "2", 2), NameTermValue("f1", "2", 2), NameTermValue("f2", "a", 0))),
        (1, Seq(NameTermValue("f1", "3", 1), NameTermValue("f2", "b", 1), NameTermValue("f2", "a", 3)))
      ).toDF("label", "features").write.mode(SaveMode.Overwrite).avro(path)
    }
    spark.stop()
  }

  /**
   * Clean up the generated data
   */
  @AfterClass
  def deleteData(): Unit = {

    val sparkConf: SparkConf = new SparkConf().set("spark.driver.bindAddress", "127.0.0.1")
    val spark = SparkSession.builder().master("local[*]").appName("delete-data").config(sparkConf).getOrCreate()
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val testDataPath = new Path(rootPath)
    if (fs.exists(testDataPath)) {
      fs.delete(testDataPath, true)
    }
    fs.close()
    spark.stop()
  }
}
