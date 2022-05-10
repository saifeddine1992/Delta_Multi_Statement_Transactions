package com.databeans

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, max}

case class TableStates(tableName: String, initialVersion: Long, latestVersion: Long)

object MultiStatementUtils {

  def createTableStates(spark: SparkSession): Unit = {
    import spark.implicits._
    val emptyConf: Seq[TableStates] = Seq()
    emptyConf.toDF().write.format("delta").mode("overwrite").saveAsTable("tableStates")
  }

  def getTableVersion(spark: SparkSession, tableName: String): Long = {
    import io.delta.tables._
    import spark.implicits._
    DeltaTable.forName(spark, tableName).history().select(max(col("version"))).as[Long].head()
  }

  def updateTableStates(spark: SparkSession, updatedTableStates: DataFrame): Unit = {
    DeltaTable.forName(spark, "tableStates").as("tableStates")
      .merge(updatedTableStates.as("updatedTableStates"),
        "tableStates.tableName = updatedTableStates.tableName")
      .whenMatched()
      .updateExpr(Map(
        "latestVersion" -> "updatedTableStates.latestVersion"))
      .whenNotMatched()
      .insertAll()
      .execute()
  }

  def getTableInitialVersion(spark: SparkSession, tableName: String): Long = {
    import spark.implicits._
    import org.apache.spark.sql.functions.col
    spark.read.format("delta").table("tableStates").select("initialVersion").where(col("tableName") === tableName).as[Long].head()
  }

  def createView(spark: SparkSession, tableName: String) : Unit = {
    spark.read.format("delta").table(tableName).createOrReplaceGlobalTempView(tableName+"_view")
  }
}
