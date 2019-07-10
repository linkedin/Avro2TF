package com.linkedin.avro2tf.jobs

import java.io.{File, FileOutputStream, PrintWriter}
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.mutable

import com.databricks.spark.avro._
import com.linkedin.avro2tf.parsers.TensorizeInJobParamsParser
import com.linkedin.avro2tf.utils.Constants.{HASH_INFO, NTV_NAME, NTV_TERM, NTV_VALUE, TMP_FEATURE_LIST}
import com.linkedin.avro2tf.utils.ConstantsForTest._
import com.linkedin.avro2tf.utils.WithLocalSparkSession

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.testng.Assert._
import org.testng.annotations.{DataProvider, Test}

class FeatureListGenerationTest extends WithLocalSparkSession {

  /**
   * Test if the Feature List Generation job can finish successfully
   *
   */
  @Test
  def testFeatureList(): Unit = {

    val tensorizeInConfig = new File(
      getClass.getClassLoader.getResource(TENSORIZEIN_CONFIG_PATH_VALUE_SAMPLE).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(WORKING_DIRECTORY_FEATURE_LIST_GENERATION_TEXT))

    // Set up external feature list
    val externalFeatureListFullPath = s"$WORKING_DIRECTORY_FEATURE_LIST_GENERATION_TEXT/$EXTERNAL_FEATURE_LIST_PATH_TEXT"
    new File(externalFeatureListFullPath).mkdirs()
    new PrintWriter(
      new FileOutputStream(
        s"$externalFeatureListFullPath/$EXTERNAL_FEATURE_LIST_FILE_NAME_TEXT",
        false)) {
      write(SAMPLE_EXTERNAL_FEATURE_LIST)
      close()
    }

    val params = Array(
      INPUT_PATHS_NAME, INPUT_TEXT_FILE_PATHS,
      WORKING_DIRECTORY_NAME, WORKING_DIRECTORY_FEATURE_LIST_GENERATION_TEXT,
      TENSORIZEIN_CONFIG_PATH_NAME, tensorizeInConfig,
      EXTERNAL_FEATURE_LIST_PATH_NAME, externalFeatureListFullPath
    )

    val dataFrame = session.read.avro(INPUT_TEXT_FILE_PATHS)
    val tensorizeInParams = TensorizeInJobParamsParser.parse(params)

    val dataFrameExtracted = (new FeatureExtraction).run(dataFrame, tensorizeInParams)
    val dataFrameTransformed = (new FeatureTransformation).run(dataFrameExtracted, tensorizeInParams)
    (new FeatureListGeneration).run(dataFrameTransformed, tensorizeInParams)

    // Check if columns of String Array type and NTV struct type have feature lists generated
    val fileSystem = FileSystem.get(session.sparkContext.hadoopConfiguration)
    val featureListPath = new Path(tensorizeInParams.workingDir.featureListPath)

    val filesIterator = fileSystem.listFiles(featureListPath, ENABLE_RECURSIVE)
    val files = new mutable.HashSet[String]
    while (filesIterator.hasNext) {
      files.add(filesIterator.next().getPath.getName)
    }
    fileSystem.close()

    val colsNeedFeatureList = tensorizeInParams.tensorizeInConfig.features
      .filter(
        feature => {
          val colName = feature.outputTensorInfo.name
          dataFrameTransformed.schema(colName).dataType match {
            case arrayType: ArrayType if arrayType.elementType.isInstanceOf[StringType] => true
            case arrayType: ArrayType if arrayType.elementType.isInstanceOf[StructType] =>
              val ntvType = arrayType.elementType.asInstanceOf[StructType]
              ntvType.fieldNames.contains(NTV_NAME) && ntvType.fieldNames.contains(NTV_TERM) &&
                ntvType.fieldNames.contains(NTV_VALUE)
            case _: StringType => true
            case _ => false
          }
        })
      .filter(
        feature => {
          feature.inputFeatureInfo.get.transformConfig match {
            case Some(config) if config.hashInfo.isDefined => false
            case _ => true
          }
        })
      .map(feature => feature.outputTensorInfo.name)
      .toSet

    assertTrue(colsNeedFeatureList.subsetOf(files))
  }

  /**
   * Data provider for testing feature list sharing settings conflicting with hashing and external feature list
   *
   */
  @DataProvider
  def testDataWithConflictingFeatureListSharing(): Array[Array[Any]] = {

    Array(
      Array("words_term,wordSeq"),
      Array("wordSeq,wordSeq_hashed")
    )
  }

  /**
   * Test that feature list generation correctly raise exception for the following conditions.
   * Case a) output tensor has corresponding external feature list
   * Case b) there are hashing configurations for the output tensor
   *
   */
  @Test(
    expectedExceptions = Array(classOf[IllegalArgumentException]),
    dataProvider = "testDataWithConflictingFeatureListSharing",
    expectedExceptionsMessageRegExp = "Settings in --tensors-sharing-feature-lists conflict with other settings.*"
  )
  def testConflictingFeatureListSharingSetting(tensors_sharing_feature_lists: String): Unit = {

    val tensorizeInConfig = new File(
      getClass.getClassLoader.getResource(TENSORIZEIN_CONFIG_PATH_VALUE_SAMPLE).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(WORKING_DIRECTORY_FEATURE_LIST_GENERATION_TEXT))

    // Set up external feature list
    val externalFeatureListFullPath = s"$WORKING_DIRECTORY_FEATURE_LIST_GENERATION_TEXT/$EXTERNAL_FEATURE_LIST_PATH_TEXT"
    new File(externalFeatureListFullPath).mkdirs()
    new PrintWriter(
      new FileOutputStream(
        s"$externalFeatureListFullPath/$EXTERNAL_FEATURE_LIST_FILE_NAME_TEXT",
        false)) {
      write(SAMPLE_EXTERNAL_FEATURE_LIST)
      close()
    }

    val params = Array(
      INPUT_PATHS_NAME, INPUT_TEXT_FILE_PATHS,
      WORKING_DIRECTORY_NAME, WORKING_DIRECTORY_FEATURE_LIST_GENERATION_TEXT,
      TENSORIZEIN_CONFIG_PATH_NAME, tensorizeInConfig,
      EXTERNAL_FEATURE_LIST_PATH_NAME, externalFeatureListFullPath,
      TENSORS_SHARING_FEATURE_LISTS_NAME, tensors_sharing_feature_lists
    )

    val dataFrame = session.read.avro(INPUT_TEXT_FILE_PATHS)
    val tensorizeInParams = TensorizeInJobParamsParser.parse(params)

    val dataFrameExtracted = (new FeatureExtraction).run(dataFrame, tensorizeInParams)
    val dataFrameTransformed = (new FeatureTransformation).run(dataFrameExtracted, tensorizeInParams)
    (new FeatureListGeneration).run(dataFrameTransformed, tensorizeInParams)
  }

  /**
   * Test the correctness of collectAndSaveFeatureList function, this is the function that collects count data for each
   * item in the feature list, and write them out into temporary files.
   *
   */
  @Test()
  def testCollectAndSaveFeatureList(): Unit = {

    val tensorizeInConfig = new File(
      getClass.getClassLoader.getResource(TENSORIZEIN_CONFIG_PATH_VALUE_SHARE_FEATURE).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(WORKING_DIRECTORY_FEATURE_LIST_GENERATION_TEXT))

    val params = Array(
      INPUT_PATHS_NAME, INPUT_SHARE_FEATURE_PATH,
      WORKING_DIRECTORY_NAME, WORKING_DIRECTORY_FEATURE_LIST_GENERATION_TEXT,
      TENSORIZEIN_CONFIG_PATH_NAME, tensorizeInConfig,
      TENSORS_SHARING_FEATURE_LISTS_NAME, TENSORS_SHARING_FEATURE_LISTS_VALUE_CASE_1
    )

    val dataFrame = session.read.avro(INPUT_SHARE_FEATURE_PATH)
    val tensorizeInParams = TensorizeInJobParamsParser.parse(params)

    val dataFrameExtracted = (new FeatureExtraction).run(dataFrame, tensorizeInParams)
    (new FeatureListGeneration).run(dataFrameExtracted, tensorizeInParams)

    // Check if correct temporary feature lists are generated
    val fileSystem = FileSystem.get(session.sparkContext.hadoopConfiguration)
    val tmpFeatureListPath = new Path(s"${tensorizeInParams.workingDir.rootPath}/$TMP_FEATURE_LIST")

    var actualTmpFeatureListDirs = Map[String, FileStatus]()
    val tmpFeatureListPathStatus = fileSystem.listStatus(tmpFeatureListPath)
    tmpFeatureListPathStatus.foreach(
      p => if (p.isDirectory()) {
        actualTmpFeatureListDirs +=
          (p.getPath.getName -> p) // dir name => dir path object; dir name contains output tensor name
      })

    // expected feature lists with counts
    val expectedTmpFeatureListsPath = new Path(
      getClass.getClassLoader.getResource(EXPECTED_FEATURE_LIST_W_COUNTS_PATH)
        .getPath) // one file for one output tensor
    val expectedFilesIterator = fileSystem.listFiles(expectedTmpFeatureListsPath, !ENABLE_RECURSIVE)
    while (expectedFilesIterator.hasNext) {
      // read in expected feature list for one output tensor
      val expectedFilePath = expectedFilesIterator.next().getPath
      val expectedFileInputStream = fileSystem.open(expectedFilePath)
      val expectedFeatureItems = scala.io.Source.fromInputStream(expectedFileInputStream, UTF_8.name())
        .getLines().toSet
      // make sure actual temporary feature list exists
      val featureListName = expectedFilePath.getName
      assertTrue(actualTmpFeatureListDirs contains featureListName)
      // get contents of temporary feature list
      val actualFilesIterator = fileSystem
        .listFiles(actualTmpFeatureListDirs(featureListName).getPath, ENABLE_RECURSIVE)
      var actualFeatureItems = new collection.mutable.HashSet[String]
      while (actualFilesIterator.hasNext) {
        actualFeatureItems ++= scala.io.Source.fromInputStream(
          fileSystem.open(actualFilesIterator.next().getPath),
          UTF_8.name())
          .getLines().toSet
      }
      // make sure contents of temporary feature list is correct
      assertTrue(actualFeatureItems == expectedFeatureItems)
    }
    fileSystem.close()
  }

  /**
   * Data provider for testing feature list sharing settings
   * case 1) all output tensors have some feature list sharing setting, 2 or 3 tensors share the same feature list
   * case 2) some output tensors have feature list sharing settings, some not
   */
  @DataProvider
  def testDataForFeatureListSharing(): Array[Array[Any]] = {

    Array(
      Array(TENSORS_SHARING_FEATURE_LISTS_VALUE_CASE_1, FEATURE_LIST_SHARING_EXPECTED_VALUE_PATH_CASE1),
      Array(TENSORS_SHARING_FEATURE_LISTS_VALUE_CASE_2, FEATURE_LIST_SHARING_EXPECTED_VALUE_PATH_CASE2)
    )
  }

  /**
   * Test the correctness of feature list generation with feature list sharing setting
   *
   */
  @Test(dataProvider = "testDataForFeatureListSharing")
  def testFeatureListWithFeatureListSharing(
    tensors_sharing_feature_lists: String,
    expected_feat_lists_dir: String): Unit = {

    val tensorizeInConfig = new File(
      getClass.getClassLoader.getResource(TENSORIZEIN_CONFIG_PATH_VALUE_SHARE_FEATURE).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(WORKING_DIRECTORY_FEATURE_LIST_GENERATION_TEXT))

    val params = Array(
      INPUT_PATHS_NAME, INPUT_SHARE_FEATURE_PATH,
      WORKING_DIRECTORY_NAME, WORKING_DIRECTORY_FEATURE_LIST_GENERATION_TEXT,
      TENSORIZEIN_CONFIG_PATH_NAME, tensorizeInConfig,
      TENSORS_SHARING_FEATURE_LISTS_NAME, tensors_sharing_feature_lists
    )

    val dataFrame = session.read.avro(INPUT_SHARE_FEATURE_PATH)
    val tensorizeInParams = TensorizeInJobParamsParser.parse(params)

    val dataFrameExtracted = (new FeatureExtraction).run(dataFrame, tensorizeInParams)
    (new FeatureListGeneration).run(dataFrameExtracted, tensorizeInParams)

    // get actual generated feature list
    val fileSystem = FileSystem.get(session.sparkContext.hadoopConfiguration)
    val featureListPath = new Path(tensorizeInParams.workingDir.featureListPath)
    val filesIterator = fileSystem.listFiles(featureListPath, ENABLE_RECURSIVE)
    var actualFeatureListFiles = Map[String, FileStatus]()
    while (filesIterator.hasNext) {
      val featureListFile = filesIterator.next()
      actualFeatureListFiles += (featureListFile.getPath.getName -> featureListFile)
    }

    // get expected feature lists with counts
    val expectedFeatureListsPath = new Path(
      getClass.getClassLoader.getResource(expected_feat_lists_dir)
        .getPath) // one file for one output tensor
    val expectedFilesIterator = fileSystem.listFiles(expectedFeatureListsPath, !ENABLE_RECURSIVE)
    while (expectedFilesIterator.hasNext) {
      // read in expected feature list for one output tensor
      val expectedFilePath = expectedFilesIterator.next().getPath
      val expectedFileInputStream = fileSystem.open(expectedFilePath)
      val expectedFeatureItems = scala.io.Source.fromInputStream(expectedFileInputStream, UTF_8.name())
        .getLines().toArray
      // get contents of feature list
      val actualFilePath = actualFeatureListFiles(expectedFilePath.getName).getPath
      val actualFileInputStream = fileSystem.open(actualFilePath)
      val actualFeatureItems = scala.io.Source.fromInputStream(actualFileInputStream, UTF_8.name())
        .getLines().toArray
      // make sure contents of feature list is correct
      assertTrue(actualFeatureItems.size == expectedFeatureItems.size)
      for ((actual, expected) <- (actualFeatureItems zip expectedFeatureItems)) {
        assertTrue(actual == expected)
      }
    }
    fileSystem.close()
  }

  /**
   * Test correctly raise exception when a NTV tensor with feature list sharing contains more than 1 name in NTV data
   *
   */
  @Test(
    expectedExceptions = Array(classOf[IllegalArgumentException]),
    expectedExceptionsMessageRegExp = "Output tensors of NTV type with feature sharing settings can only have 1 value.*"
  )
  def testExceptionNonUniqueNameForNTVSharingFeatureList(): Unit = {

    val tensorizeInConfig = new File(
      getClass.getClassLoader.getResource(TENSORIZEIN_CONFIG_PATH_VALUE_SHARE_FEATURE).getFile
    ).getAbsolutePath
    FileUtils.deleteDirectory(new File(WORKING_DIRECTORY_FEATURE_LIST_GENERATION_TEXT))

    val params = Array(
      INPUT_PATHS_NAME, INPUT_SHARE_FEATURE_PATH,
      WORKING_DIRECTORY_NAME, WORKING_DIRECTORY_FEATURE_LIST_GENERATION_TEXT,
      TENSORIZEIN_CONFIG_PATH_NAME, tensorizeInConfig,
      TENSORS_SHARING_FEATURE_LISTS_NAME, TENSORS_SHARING_FEATURE_LISTS_VALUE_CASE_3
    )

    val dataFrame = session.read.avro(INPUT_SHARE_FEATURE_PATH)
    val tensorizeInParams = TensorizeInJobParamsParser.parse(params)

    val dataFrameExtracted = (new FeatureExtraction).run(dataFrame, tensorizeInParams)
    (new FeatureListGeneration).run(dataFrameExtracted, tensorizeInParams)
  }
}