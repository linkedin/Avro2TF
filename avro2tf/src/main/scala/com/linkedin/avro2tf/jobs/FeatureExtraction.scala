package com.linkedin.avro2tf.jobs

import com.linkedin.avro2tf.configs.Feature
import com.linkedin.avro2tf.helpers.TensorizeInConfigHelper
import com.linkedin.avro2tf.parsers.TensorizeInParams
import com.linkedin.avro2tf.utils.{CommonUtils, Constants}
import com.linkedin.avro2tf.utils.Constants._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, expr, struct, udf}
import org.apache.spark.sql.{Column, DataFrame, Row}

/**
 * The Feature Extraction job extracts features and labels that will be converted to tensors and later used in training.
 *
 */
class FeatureExtraction {

  /**
   * The main function to perform Feature Extraction job
   *
   * @param dataFrame Input data Spark DataFrame
   * @param params TensorizeIn parameters specified by user
   * @return A Spark DataFrame
   */
  def run(dataFrame: DataFrame, params: TensorizeInParams): DataFrame = {

    // Concatenate features and labels of TensorizeIn configuration
    val featuresAndLabels = TensorizeInConfigHelper.concatFeaturesAndLabels(params)

    // Get a sequence of tensor columns based on column expressions and column configurations from input feature information
    val tensorColumns = getTensorColumns(dataFrame, featuresAndLabels)

    // Get the extra columns to keep specified by user
    val extraColumns = getExtraColumns(params)

    dataFrame.select(tensorColumns ++ extraColumns: _*)
  }

  /**
   * Get the extra columns that users want to keep besides the tensor columns
   *
   * @param params TensorizeIn parameters specified by user
   * @return A Spark DataFrame
   */
  private def getExtraColumns(params: TensorizeInParams): Seq[Column] = {
    params.extraColumnsToKeep.map {
      exprString => if(exprString.contains(Constants.COLUMN_NAME_ALIAS_DELIMITER)){
        val exprAndName = exprString.trim.split(Constants.COLUMN_NAME_ALIAS_DELIMITER)
        expr(exprAndName.head).alias(exprAndName.last)
      } else {
        expr(exprString)
      }
    }
  }

  /**
   * Get a sequence of tensor columns based on column expressions and column configurations from input feature information
   *
   * @param dataFrame Input data Spark DataFrame
   * @param featuresAndLabels A sequence of features and labels from TensorizeIn configuration
   * @return A sequence of Spark columns
   */
  private def getTensorColumns(dataFrame: DataFrame, featuresAndLabels: Seq[Feature]): Seq[Column] = {

    // Get all existing columns from the input Spark DataFrame
    val existingColumns = dataFrame.columns.map(col)

    // Create a struct as input column based on all existing columns
    val inputColumn = struct(existingColumns: _*)

    featuresAndLabels.map {
      featureOrLabel => {
        val inputFeatureInfo = featureOrLabel.inputFeatureInfo.get
        val columnExprOpt = inputFeatureInfo.columnExpr
        val columnConfigOpt = inputFeatureInfo.columnConfig
        val columnName = featureOrLabel.outputTensorInfo.name

        columnExprOpt match {
          // Apply column expression and rename column
          case Some(columnExpr) => expr(columnExpr).alias(columnName)
          // Apply a Spark UDF function to extract NameTermValue (NTV) features and rename column
          case None => extractNTVFeatures(columnConfigOpt.get)(inputColumn).alias(columnName)
        }
      }
    }
  }

  /**
   * A Spark UserDefinedFunction to extract NameTermValue (NTV) features using column configuration in input feature information
   *
   * @param columnConfig Column configuration in input feature information
   * @return A Spark UserDefinedFunction
   */
  private def extractNTVFeatures(columnConfig: Map[String, Map[String, Seq[String]]]): UserDefinedFunction = {

    udf {
      row: Row => {
        columnConfig.flatMap {
          // Get feature bag name and information on features to extract from column configuration
          case (featureBagName, featuresToExtract) =>
            // Get a sequence of NameTermValue (NTV) rows by using the feature bag name in column configuration
            val nameTermValues = row.getAs[Seq[Row]](featureBagName)

            if (nameTermValues != null) {
              nameTermValues.filter {
                nameTermValue => {
                  // Get the name from a NameTermValue (NTV)
                  val name = nameTermValue.getAs[String](NTV_NAME)

                  if (featuresToExtract.contains(WHITELIST)) {
                    featuresToExtract(WHITELIST).contains(WILD_CARD) ||
                      featuresToExtract(WHITELIST).exists(name.matches)
                  } else {
                    if (featuresToExtract(BLACKLIST).contains(WILD_CARD)) {
                      false
                    } else {
                      !featuresToExtract(BLACKLIST).exists(name.matches)
                    }
                  }
                }
              }.map {
                nameTermValue =>
                  // Construct a NameTermValue (NTV) tuple for later conversion to SparseVector
                  TensorizeIn.NameTermValue(
                    nameTermValue.getAs[String](NTV_NAME),
                    nameTermValue.getAs[String](NTV_TERM),
                    // Use a common utility function to convert any data type to float
                    CommonUtils.convertValueOfNTVToFloat(nameTermValue)
                  )
              }
            } else {
              Seq.empty[TensorizeIn.NameTermValue]
            }
        }.toSeq
      }
    }
  }
}