package org.vigil.bonus

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.functions.{col, first, lit, when}
import org.apache.spark.sql.types.IntegerType


object GroupOddsBonus {
  // Using lit to get column the first and the second column and rename then
  // grouping by key and value
  // filter the odds different than zero division
  // agg the value from the odd
  // withColumn odd_value as an Integer
  def execute(df: DataFrame): DataFrame = {
    df.withColumn("key", when(col("key") === "", lit("0")).otherwise(col("key")))
      .withColumn("value", when(col("value") === "", lit("1")).otherwise(col("value")))
      .groupBy("key", "value")
      .count()
      .filter("count % 2 != 0")
      .groupBy("key")
      .agg(first("value").as("odd_value"))
      .withColumn("key", col("key").cast(IntegerType))
      .withColumn("odd_value", col("odd_value").cast(IntegerType))
  }

}
