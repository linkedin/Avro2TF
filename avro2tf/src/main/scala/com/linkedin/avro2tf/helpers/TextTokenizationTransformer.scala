package com.linkedin.avro2tf.helpers

import com.linkedin.avro2tf.configs.Tokenization
import com.linkedin.avro2tf.parsers.TensorizeInParams
import com.linkedin.avro2tf.utils.Constants._
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
 * Helper file for tokenization on text feature in Feature Transformation
 *
 */
object TextTokenizationTransformer {

  /**
   * Tokenize text feature
   *
   * @param dataFrame Input data Spark DataFrame
   * @param params TensorizeIn parameters specified by user
   * @return A Spark DataFrame
   */
  def tokenizeTextFeature(dataFrame: DataFrame, params: TensorizeInParams): sql.DataFrame = {

    var tokenizedDataFrame = dataFrame

    TensorizeInConfigHelper.concatFeaturesAndLabels(params)
      .foreach(featureOrLabel => {
        featureOrLabel.inputFeatureInfo.get.transformConfig match {
          case Some(transformConfig) if transformConfig.tokenization.isDefined =>
            val outputColName = featureOrLabel.outputTensorInfo.name
            val regexedColName = s"$outputColName-$REGEXED_COLUMN_NAME_SUFFIX"

            // Text feature tokenization with regular expression
            tokenizedDataFrame = regexTokenization(dataFrame, regexedColName, outputColName)

            // Tokenize text feature with removing stop words
            tokenizedDataFrame = tokenizeWithRemoveStopWords(tokenizedDataFrame, transformConfig.tokenization.get, regexedColName, outputColName)
          case _ =>
        }
      })

    tokenizedDataFrame
  }

  /**
   * Tokenize text feature with information on removing stop words
   *
   * @param dataFrame Input data Spark DataFrame
   * @param tokenizationConfig Optional tokenization configuration in transformation configuration
   * @param regexedColName Output column name with a regexed suffix
   * @param outputColName Name in output tensor information
   * @return A Spark DataFrame
   */
  private def tokenizeWithRemoveStopWords(
    dataFrame: DataFrame,
    tokenizationConfig: Tokenization,
    regexedColName: String,
    outputColName: String): DataFrame = {

    val tokenizedDataFrame = if(tokenizationConfig.removeStopWords)
        // Text feature tokenization with information on removing stop words
        removeStopWordsTokenization(dataFrame, regexedColName, outputColName)
    else
        // Rename the regexed column with the name in output tensor information
        dataFrame.withColumn(outputColName, col(regexedColName))

    // Drop the regexed column
    tokenizedDataFrame.drop(regexedColName)
  }

  /**
   * Text feature tokenization with a regular expression
   *
   * @param dataFrame Input data Spark DataFrame
   * @param regexedColName Output column name with a regex suffix
   * @param outputColName Name in output tensor information
   * @return A Spark DataFrame
   */
  private def regexTokenization(
    dataFrame: DataFrame,
    regexedColName: String,
    outputColName: String): DataFrame = {

    // Get a regular expression tokenizer with Spark MLlib
    val regexTokenizer: RegexTokenizer = getRegexTokenizer(outputColName, regexedColName)

    regexTokenizer.transform(dataFrame)
  }

  /**
   * Text feature tokenization using information on removing stop words
   *
   * @param dataFrame Input data Spark DataFrame
   * @param regexedColName Column name with a regexed suffix
   * @param outputColName Name in output tensor information
   * @return A Spark DataFrame
   */
  private def removeStopWordsTokenization(
    dataFrame: DataFrame,
    regexedColName: String,
    outputColName: String): DataFrame = {

    var tokenizedDataFrame = dataFrame
    val removerColName = s"$outputColName-$REMOVER_COLUMN_NAME_SUFFIX"

    // Get a stop words remover with Spark MLlib
    val stopWordsRemover: StopWordsRemover = getStopWordsRemover(regexedColName, removerColName)

    tokenizedDataFrame = stopWordsRemover.transform(tokenizedDataFrame)
    // Rename the remover column with output column name and drop it
    tokenizedDataFrame = tokenizedDataFrame.withColumn(outputColName, col(removerColName)).drop(removerColName)

    tokenizedDataFrame
  }

  /**
   * Get a regular expression tokenizer with Spark MLlib
   *
   * @param inputCol Input column name
   * @param outputCol Output column name
   * @return A Spark MLlib RegexTokenizer
   */
  private def getRegexTokenizer(inputCol: String, outputCol: String): RegexTokenizer = {

    new RegexTokenizer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setPattern(DEFAULT_TOKENIZER_DELIMITER_REGEX)
  }

  /**
   * Get a stop words remover with Spark MLlib
   *
   * @param inputCol Input column name
   * @param outputCol Output column name
   * @return A Spark MLlib StopWordsRemover
   */
  private def getStopWordsRemover(inputCol: String, outputCol: String): StopWordsRemover = {

    new StopWordsRemover()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
  }
}