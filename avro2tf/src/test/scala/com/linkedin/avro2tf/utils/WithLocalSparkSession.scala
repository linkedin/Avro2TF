package com.linkedin.avro2tf.utils

import java.lang.reflect.Method

import com.linkedin.avro2tf.utils.ConstantsForTest._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.testng.annotations.{AfterMethod, BeforeMethod}

/**
 * This trait creates a local spark session for each test method of a test class that extends this trait
 *
 */
trait WithLocalSparkSession {
  @transient var session: SparkSession = _

  @BeforeMethod
  def setup(method: Method) {

    session = getSparkSession(method.getName)
  }

  @AfterMethod
  def tearDown(): Unit = {

    session.stop()
    session = null
  }

  private def getSparkSession(name: String): SparkSession = {

    val sparkConf = new SparkConf()
      .registerKryoClasses(Array())
      .set(SPARK_DRIVER_BIND_ADDRESS_NAME, SPARK_DRIVER_BIND_ADDRESS_VALUE)
      .set("spark.sql.avro.compression.codec", "deflate")
      .set("spark.sql.avro.deflate.level", "5")

    SparkSession.builder
      .master(SPARK_SESSION_BUILDER_MASTER)
      .appName(name)
      .config(sparkConf)
      .getOrCreate()
  }
}