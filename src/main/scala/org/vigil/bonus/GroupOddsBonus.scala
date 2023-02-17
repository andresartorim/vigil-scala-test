package org.vigil.bonus

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.functions.{col, first, lit, when}
import org.apache.spark.sql.types.IntegerType


object GroupOddsBonus {
  // groupBy keys and filter odds
  def execute(df: DataFrame): DataFrame = {
    df
      .withColumnRenamed(df.columns.head, "key")
      .withColumnRenamed(df.columns.last, "value")
      .groupBy("key", "value")
      .agg(functions.count("value"))
      .filter("count(value) % 2 != 0")
      .withColumn("value", col("value").cast(IntegerType))
      .select("key", "value")
  }

}
