package com.databeans

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, max, min}

case class TableStates(transaction_id : Int, tableName: String, initialVersion: Long, latestVersion: Long)

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
        "tableStates.transaction_id = updatedTableStates.transaction_id")
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

  def getTableLatestVersion(spark: SparkSession, tableName: String): Long = {
    import spark.implicits._
    import org.apache.spark.sql.functions.col
    spark.read.format("delta").table("tableStates").select("latestVersion").where(col("tableName") === tableName).as[Long].head()
  }

  def createView(spark: SparkSession, tableName: String): Unit = {
    spark.read.format("delta").table(tableName).createOrReplaceTempView(tableName + "_view")
  }

  def beginTransaction(spark: SparkSession, transactions: Array[String], tableNames: Array[String]): Unit = {
    import spark.implicits._

    createTableStates(spark)
    for (i <- tableNames.indices) {
      createView(spark, tableNames(i))
    }
    for (j <- transactions.indices) {
      val initialVersion = getTableVersion(spark, tableNames(j))
      spark.sql(transactions(j))
      print(s"transaction s'${j} committed")
      val latestVersion = getTableVersion(spark, tableNames(j))
      val updatedTableStates = Seq(TableStates(j, tableNames(j), initialVersion, latestVersion)).toDF()
      updateTableStates(spark, updatedTableStates)
      if (j == (transactions.indices.length - 1)) {
        for (k <- tableNames.indices) {
          createView(spark, tableNames(k))
        }
      }
    }
  }

  def createViewsInCaseOfFailure(spark: SparkSession, tableNames: Array[String]): Unit = {
    import spark.implicits._
    for (i <- tableNames.indices) {
      val initialVersion = spark.read.format("delta")
        .table("tableStates")
        .where(col("tableName") === tableNames(i))
        .select(min(col("initialVersion")))
        .as[Long].head()
      spark.read.format("delta").option("versionAsOf", initialVersion).table(tableNames(i)).createOrReplaceTempView(tableNames(i) + "_view")
    }
  }

  def rerunQueries(spark: SparkSession, transactions: Array[String], tableNames: Array[String]): Unit = {
    import spark.implicits._
    for (i <- transactions.indices) {
      if (getTableVersion(spark, tableNames(i)) > getTableInitialVersion(spark, tableNames(i))) {
        print(s"transaction ${i} already performed")
        val updatedTableStates = Seq(TableStates(i, tableNames(i), getTableInitialVersion(spark, tableNames(i)), getTableVersion(spark, tableNames(i)))).toDF()
        updateTableStates(spark, updatedTableStates)
      }
      else {
        val initialVersion = getTableVersion(spark, tableNames(i))
        spark.sql(transactions(i))
        print(s"transaction ${i} committed")
        val latestVersion = getTableVersion(spark, tableNames(i))
        val updatedTableStates = Seq(TableStates(i, tableNames(i), initialVersion, latestVersion)).toDF()
        updateTableStates(spark, updatedTableStates)
      }
      if (i == (transactions.indices.length - 1)) {
        for (j <- tableNames.indices) {
          createView(spark, tableNames(j))
        }
      }
    }
  }
}
