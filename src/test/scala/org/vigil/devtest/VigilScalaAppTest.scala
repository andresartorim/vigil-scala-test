package org.vigil.devtest
import org.apache.spark.sql.SparkSession
import org.vigil.commons.AppFunctions.readFiles

class VigilScalaAppTest extends org.scalatest.funsuite.AnyFunSuite {

  test("should get key a/nd value odds from tsv and csv files") {
    VigilScalaApp.main(Array("src/test/resources/samples", "src/test/resources/output/default", "default"))
    val spark: SparkSession = SparkSession.builder()
      .appName("VigilScalaAppTest")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
      .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
      .config("spark.hadoop.fs.s3a.aws.credentials.profile", "default")
      .getOrCreate()

    val expectedDF = readFiles(spark, "src/test/resources/expected/default", "csv", "\t")

    val df = readFiles(spark, "src/test/resources/output/default", "csv", "\t")
    assert(df.collect().sameElements(expectedDF.collect()))
  }

}
