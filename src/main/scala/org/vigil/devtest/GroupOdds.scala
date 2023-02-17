package org.vigil.devtest

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.functions.{col, first, lit, when}
import org.apache.spark.sql.types.IntegerType


object GroupOdds {

  def execute(df: DataFrame): DataFrame = {
    df.withColumn("value", when(col("value") === "", lit("0")).otherwise(col("value")))
      .groupBy("key", "value")
      .count()
      .filter("count % 2 != 0")
      .groupBy("key")
      .agg(first("value").as("odd_value"))
      .withColumn("odd_value", col("odd_value").cast(IntegerType))
  }

}
