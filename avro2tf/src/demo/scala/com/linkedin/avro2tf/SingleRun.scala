package com.linkedin.avro2tf

import com.linkedin.avro2tf.constants.Avro2TFJobParamNames
import com.linkedin.avro2tf.jobs.Avro2TF
import com.linkedin.avro2tf.parsers.{Avro2TFJobParamsParser, Avro2TFParams}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * This is a simple test job that can run locally to process small aount of data
 * e.g., SingleRun --input-paths  "avro2tf/src/demo/data/trainData.avro"
                    --working-dir "/tmp/avro2tf-test-movielens"
                    --avro2tf-config-path "avro2tf/src/demo/config/avro2tf_config_movielens.json"
                   --output-format "tfrecord"
 */
object SingleRun {
  def main(args: Array[String]) = {
    val session = getSparkSession("single run")
    // replace this line with parse from map if we have a parameters mapped.
    val params = Avro2TFJobParamsParser.parse(args)
    println(params)
    Avro2TF.run(session, params)
    session.stop()
  }

  def parseParamsFromMap(): Avro2TFParams = {
        val paramsMap = Map(
          Avro2TFJobParamNames.INPUT_PATHS -> "avro2tf/src/demo/data/trainData.avro",
          Avro2TFJobParamNames.WORKING_DIR -> "/tmp/avro2tf-test-movielens",
          Avro2TFJobParamNames.AVRO2TF_CONFIG_PATH -> "avro2tf/src/demo/config/avro2tf_config_movielens.json",
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