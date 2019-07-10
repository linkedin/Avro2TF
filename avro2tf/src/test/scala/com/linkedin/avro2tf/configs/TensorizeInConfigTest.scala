package com.linkedin.avro2tf.configs

import org.testng.annotations.Test

import org.testng.Assert._

class TensorizeInConfigTest {
  @Test
  def testEquality(): Unit = {
    val array1 = Array(1)
    val array2 = Array(1)

    val first = OutputTensorInfo("foo", DataType.int, array1)
    val second = OutputTensorInfo("foo", DataType.int, array2)

    assertEquals(first, second)
  }
}
