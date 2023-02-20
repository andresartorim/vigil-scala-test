package org.vigil.bonus

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.vigil.commons.AppFunctions.readFiles

class GroupOddsBonusTest extends org.scalatest.funsuite.AnyFunSuite  with BeforeAndAfterAll {
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("GroupOddsBonusTest")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
      .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
      .config("spark.hadoop.fs.s3a.aws.credentials.profile", "default")
      .getOrCreate()
  }


  test("should group odds using tsv and csv") {
    val csvDF = readFiles(spark, "src/test/resources/samples", "csv", ",")
    val tsvDF = readFiles(spark, "src/test/resources/samples", "tsv", "\t")
    val expectedDF = readFiles(spark, "src/test/resources/expected/bonus", "csv", "\t")

    val unionDF = csvDF.union(tsvDF)

    val groupedDF = GroupOddsBonus.execute(unionDF)
    groupedDF.show()
    assert(groupedDF.collect().sameElements(expectedDF.collect()))

  }
}
