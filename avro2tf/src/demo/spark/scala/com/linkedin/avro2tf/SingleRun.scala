package com.linkedin.avro2tf

import com.linkedin.avro2tf.constants.Avro2TFJobParamNames
import com.linkedin.avro2tf.jobs.Avro2TF
import com.linkedin.avro2tf.parsers.{Avro2TFJobParamsParser, Avro2TFParams}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * This is a simple job that can run locally to process small aount of data
 * This job consists of 3 runs:
 * 1. generate training data and meta data and feature mapping
 * 2. generate validation data using meta data
 * 3. generate test data using meta data
 */
object SingleRun {
  def main(args: Array[String]) = {
    val session = getSparkSession("single run")

    val trainParams = parseTrainParamsFromMap()
    println(trainParams)
    Avro2TF.run(session, trainParams)

    val validateParams = parseValidateParamsFromMap()
    println(validateParams)
    Avro2TF.run(session, validateParams)

    val testParams = parseTestParamsFromMap()
    println(testParams)
    Avro2TF.run(session, testParams)

    session.stop()
  }

  def parseTrainParamsFromMap(): Avro2TFParams = {
    val paramsMap = Map(
      Avro2TFJobParamNames.INPUT_PATHS -> "avro2tf/src/demo/spark/data/train",
      Avro2TFJobParamNames.WORKING_DIR -> "avro2tf/src/demo/tensorflow/data",
      Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> "avro2tf/src/demo/spark/config/avro2tf_config_movielens.json",
      Avro2TFJobParamNames.OUTPUT_FORMAT -> "tfrecord"
    )
    val prams = paramsMap.flatMap(x => Seq(s"--${x._1}", x._2.toString)).toSeq
    println(prams)
    Avro2TFJobParamsParser.parse(prams)
  }

  def parseValidateParamsFromMap(): Avro2TFParams = {
    val paramsMap = Map(
      Avro2TFJobParamNames.INPUT_PATHS -> "avro2tf/src/demo/spark/data/validate",
      Avro2TFJobParamNames.WORKING_DIR -> "avro2tf/src/demo/tensorflow/data",
      Avro2TFJobParamNames.EXECUTION_MODE -> "validation",
      Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> "avro2tf/src/demo/spark/config/avro2tf_config_movielens.json",
      Avro2TFJobParamNames.OUTPUT_FORMAT -> "tfrecord"
    )
    val prams = paramsMap.flatMap(x => Seq(s"--${x._1}", x._2.toString)).toSeq
    println(prams)
    Avro2TFJobParamsParser.parse(prams)
  }

  def parseTestParamsFromMap(): Avro2TFParams = {
    val paramsMap = Map(
      Avro2TFJobParamNames.INPUT_PATHS -> "avro2tf/src/demo/spark/data/test",
      Avro2TFJobParamNames.WORKING_DIR -> "avro2tf/src/demo/tensorflow/data",
      Avro2TFJobParamNames.EXECUTION_MODE -> "test",
      Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> "avro2tf/src/demo/spark/config/avro2tf_config_movielens.json",
      Avro2TFJobParamNames.OUTPUT_FORMAT -> "tfrecord"
    )
    val prams = paramsMap.flatMap(x => Seq(s"--${x._1}", x._2.toString)).toSeq
    println(prams)
    Avro2TFJobParamsParser.parse(prams)
  }

  private def getSparkSession(name: String): SparkSession = {
    val sparkConf = new SparkConf()
      .registerKryoClasses(Array())
      .set("spark.driver.bindAddress", "127.0.0.1")
      .set("spark.sql.avro.compression.codec", "deflate")
      .set("spark.sql.avro.deflate.level", "5")

    SparkSession.builder
      .master("local[*]")
      .appName(name)
      .config(sparkConf)
      .getOrCreate()
  }
}