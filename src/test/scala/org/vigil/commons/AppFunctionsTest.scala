package org.vigil.commons

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.vigil.commons.AppFunctions.{readFiles, saveTSVFile}
class AppFunctionsTest extends org.scalatest.funsuite.AnyFunSuite with BeforeAndAfterAll {
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("AppFunctionsTest")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
      .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
      .config("spark.hadoop.fs.s3a.aws.credentials.profile", "default")
      .getOrCreate()
  }


  test("should read file") {
    val df = readFiles(spark, "src/test/resources/expected/default", "csv", "\t")
    df.count()
    df.show()
    assert(df.count() != 0)
    assert(df.head.get(0) === 5)
    assert(df.head.get(1) === 25)
  }


  test("should save df file") {
    val expectedDF = readFiles(spark, "src/test/resources/expected/default", "csv", "\t")

    saveTSVFile(expectedDF, "src/test/resources/testResults/")

    val savedDF = readFiles(spark, "src/test/resources/testResults/", "csv", "\t")

    assert(expectedDF.collect().sameElements(savedDF.collect()))
  }

}
