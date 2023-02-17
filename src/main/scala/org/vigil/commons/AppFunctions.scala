package org.vigil.commons

import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.functions.{col, first, lit, when}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}


object AppFunctions {
  // function to read the files if it is csv or tsv, using a path as parameter
  def readFiles(spark: SparkSession,
                path: String,
                mode: String,
                delimiter: String): DataFrame = {
    spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", delimiter)
      .option("inferSchema", "true")
      .load(s"${path}/*.${mode}")
  }

  // save file into tsv format
  def saveTSVFile(result: DataFrame, output: String): Unit = {
    result
      .write
      .option("header", "true")
      .option("delimiter", "\t")
      .mode(Overwrite)
      .format("csv")
      .save(s"${output}")
  }
}
