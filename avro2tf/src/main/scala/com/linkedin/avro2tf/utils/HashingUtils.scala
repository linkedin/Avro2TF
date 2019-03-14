package com.linkedin.avro2tf.utils

import java.math.BigInteger
import java.security.MessageDigest

object HashingUtils {

  // Fix our seed and _saltLength
  val _seed: Long = 817294693
  val _saltLength: Int = 16

  // Pre-compute a frequently used salt
  val _globalSalt: String = new scala.util.Random(_seed).alphanumeric.take(_saltLength).mkString

  /**
   * Multi-hash function
   *
   * @param stringToHash The string we want to hash
   * @param numHashFunctions The number of hash functions to use
   * @param numBuckets The number of buckets of each hash function
   */
  def multiHash(stringToHash: String, numHashFunctions: Int, numBuckets: Int): Seq[Int] = {

    getSalts(numHashFunctions).map(salt => md5(stringToHash, salt, numBuckets))
  }

  /**
   * Generates a sequence of salts to be used for hashing
   *
   * @param numSalts The number of salts to return
   */
  private def getSalts(numSalts: Int): Seq[String] = {

    if (numSalts == 1) {
      Seq(_globalSalt)
    } else {
      val randomGenerator = new scala.util.Random(_seed)
      (1 to numSalts).map(_ => randomGenerator.alphanumeric.take(_saltLength).mkString)
    }
  }

  /**
   * Hashes a string using MD5
   *
   * @param stringToHash The string we want to hash
   * @param salt The salt to use while hashing
   * @param hashSize The maximum hash size
   * @return The hashed string as an integer index
   */
  private def md5(stringToHash: String, salt: String, hashSize: Int): Int = {

    val bytes = MessageDigest.getInstance("MD5").digest((stringToHash + salt).getBytes("UTF-8"))
    val bigInt = new BigInteger(1, bytes)
    bigInt.mod(BigInteger.valueOf(hashSize)).intValue()
  }
}
