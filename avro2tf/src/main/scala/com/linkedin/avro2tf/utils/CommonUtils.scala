package com.linkedin.avro2tf.utils

import com.linkedin.avro2tf.configs.DataType
import com.linkedin.avro2tf.utils.Constants._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object CommonUtils {
  /**
   * Util function to convert the data type of the value in NameTermValue to float
   *
   * @param nameTermValue NameTermValue in Row type
   * @return A float type value
   */
  def convertValueOfNTVToFloat(nameTermValue: Row): Float = {

    val dataType = nameTermValue.schema(NTV_VALUE).dataType

    dataType match {
      case DataTypes.FloatType => nameTermValue.getAs[Float](NTV_VALUE)
      case DataTypes.DoubleType => nameTermValue.getAs[Double](NTV_VALUE).toFloat
      case DataTypes.LongType => nameTermValue.getAs[Long](NTV_VALUE).toFloat
      case DataTypes.IntegerType => nameTermValue.getAs[Int](NTV_VALUE).toFloat
      case DataTypes.ShortType => nameTermValue.getAs[Short](NTV_VALUE).toFloat
      case DataTypes.ByteType => nameTermValue.getAs[Byte](NTV_VALUE).toFloat
      case _ => throw new Exception(s"Value type ${dataType.typeName} is not supported.")
    }
  }

  /**
   * Check if the type of a column is an (nested) array of name-term-value
   *
   * @param dataType The schema type of a column
   * @return is array of NTV or not
   */
  def isArrayOfNTV(dataType: DataType): Boolean = {

    dataType match {
      case arrayType: ArrayType =>
        arrayType.elementType match {
          case ntvType: StructType => ntvType.fieldNames.length == 3 && ntvType.fieldNames.contains(NTV_NAME) &&
            ntvType.fieldNames.contains(NTV_TERM) && ntvType.fieldNames.contains(NTV_VALUE)
          case arrayType: ArrayType => isArrayOfNTV(arrayType)
          case _ => false
        }
      case _ => false
    }
  }

  /**
   * Check if the type of a column is an (nested) array of sparse vector
   *
   * @param dataType The schema type of a column
   * @return is array of [[com.linkedin.avro2tf.jobs.TensorizeIn.SparseVector]]
   */
  def isArrayOfSparseTensor(dataType: DataType): Boolean = {

    dataType match {
      case arrayType: ArrayType =>
        arrayType.elementType match {
          case ntvType: StructType => ntvType.fieldNames.length == 2 && ntvType.fieldNames.contains(INDICES) &&
            ntvType.fieldNames.contains(VALUES)
          case arrayType: ArrayType => isArrayOfNTV(arrayType)
          case _ => false
        }
      case _ => false
    }
  }

  /**
   * Check if the type of a column is an (nested) array of String
   *
   * @param dataType The schema type of a column
   * @return is array of String or not
   */
  def isArrayOfString(dataType: DataType): Boolean = {

    dataType match {
      case arrayType: ArrayType =>
        arrayType.elementType match {
          case _: StringType => true
          case arrayType: ArrayType => isArrayOfString(arrayType)
          case _ => false
        }
      case _ => false
    }
  }

  /**
   * Check if the type of a column is an (nested) array of Integer
   *
   * @param dataType The schema type of a column
   * @return is array of Integer or not
   */
  def isArrayOfInteger(dataType: DataType): Boolean = {

    dataType match {
      case arrayType: ArrayType =>
        arrayType.elementType match {
          case _: IntegerType => true
          case arrayType: ArrayType => isArrayOfInteger(arrayType)
          case _ => false
        }
      case _ => false
    }
  }

  /**
   * Check if the type of a column is an (nested) array of Long
   *
   * @param dataType The schema type of a column
   * @return is array of Long or not
   */
  def isArrayOfLong(dataType: DataType): Boolean = {

    dataType match {
      case arrayType: ArrayType =>
        arrayType.elementType match {
          case _: LongType => true
          case arrayType: ArrayType => isArrayOfLong(arrayType)
          case _ => false
        }
      case _ => false
    }
  }

  /**
   * Check if the type of a column is an (nested) array of Float
   *
   * @param dataType The schema type of a column
   * @return is array of Float or not
   */
  def isArrayOfFloat(dataType: DataType): Boolean = {

    dataType match {
      case arrayType: ArrayType =>
        arrayType.elementType match {
          case _: FloatType => true
          case arrayType: ArrayType => isArrayOfFloat(arrayType)
          case _ => false
        }
      case _ => false
    }
  }

  /**
   * Check if the type of a column is an (nested) array of Float
   *
   * @param dataType The schema type of a column
   * @return is array of Float or not
   */
  def isArrayOfDouble(dataType: DataType): Boolean = {

    dataType match {
      case arrayType: ArrayType =>
        arrayType.elementType match {
          case _: DoubleType => true
          case arrayType: ArrayType => isArrayOfDouble(arrayType)
          case _ => false
        }
      case _ => false
    }
  }

  /**
   * Check if the type of a column is an (nested) array of Numerical type
   *
   * @param dataType The schema type of a column
   * @return is array of Numerical or not
   */
  def isArrayOfNumericalType(dataType: DataType): Boolean = {

    dataType match {
      case arrayType: ArrayType =>
        arrayType.elementType match {
          case _: NumericType => true
          case arrayType: ArrayType => isArrayOfNumericalType(arrayType)
          case _ => false
        }
      case _ => false
    }
  }

  /**
   * Check if a tensor is integer tensor
   *
   * @param tensorType The data type of a tensor
   * @return If the tensor is integer tensor
   */
  def isIntegerTensor(tensorType: DataType.Value): Boolean = {
    tensorType == DataType.int || tensorType == DataType.long
  }
}
