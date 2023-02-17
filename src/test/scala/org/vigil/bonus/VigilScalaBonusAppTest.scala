package org.vigil.bonus

import org.apache.spark.sql.SparkSession
import org.vigil.commons.AppFunctions.readFiles

class VigilScalaBonusAppTest extends org.scalatest.funsuite.AnyFunSuite {

  test("should get key a/nd value odds from tsv and csv files") {
    VigilScalaBonusApp.main(Array("src/test/resources/samples", "src/test/resources/output/bonus", "default"))
    val spark: SparkSession = SparkSession.builder()
      .appName("VigilScalaBonusAppTest")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
      .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
      .config("spark.hadoop.fs.s3a.aws.credentials.profile", "default")
      .getOrCreate()

    val expectedDF = readFiles(spark, "src/test/resources/expected/bonus", "csv", "\t")

    val df = readFiles(spark, "src/test/resources/output/bonus", "csv", "\t")
    assert(df.collect().sameElements(expectedDF.collect()))
  }

}
