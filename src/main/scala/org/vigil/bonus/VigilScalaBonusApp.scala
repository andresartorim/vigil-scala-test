package org.vigil.bonus

import org.apache.spark.sql.SparkSession
import org.vigil.commons.AppFunctions.{readFiles, saveTSVFile}

object VigilScalaBonusApp {


  def main(args: Array[String]): Unit = {
    val inputS3Path = args(0)
    val outputS3Path = args(1)
    val awsCredentialsProfile = args(1)


    val spark = SparkSession.builder()
      .appName("VigilScalaBonusApp")
      .master("local")
      .config("spark.driver.host","127.0.0.1")
      .config("spark.driver.bindAddress","127.0.0.1")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
      .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
      .config("spark.hadoop.fs.s3a.aws.credentials.profile", awsCredentialsProfile)
      .getOrCreate()

    // handle CSV files
    val csvDF = readFiles(spark, inputS3Path, "csv", ",")

    // handle TSV files
    val tsvDF = readFiles(spark, inputS3Path, "tsv", "\t")

    // join all the files into one dataframe
    val df = csvDF.union(tsvDF)
    df.show(2000)

    // find a value occurring an odd number of times grouping by key
    val result = GroupOddsBonus.execute(df)

    // write the output file with TSV delimiter
    saveTSVFile(result, outputS3Path)
  }
}
