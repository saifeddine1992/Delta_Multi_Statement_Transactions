package com.databeans

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, max}
import scala.util.Try

case class TableStates(transaction_id: Int, tableName: String, initialVersion: Long, latestVersion: Long)

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
    Try {
      spark.read.format("delta").table("tableStates").select("initialVersion").where(col("tableName") === tableName).as[Long].head()
    }.getOrElse(0L)
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
      runAndRegisterQuery(spark, tableNames, transactions(j), j)
      if (j == (transactions.indices.length - 1)) {
        for (k <- tableNames.indices) {
          createView(spark, tableNames(k))
        }
      }
    }
  }

  def rerunQueries(spark: SparkSession, transactions: Array[String], tableNames: Array[String]): Unit = {
    import spark.implicits._

    for (i <- transactions.indices) {
      if (getTableVersion(spark, tableNames(i)) > getTableInitialVersion(spark, tableNames(i))) {
        print(s"transaction ${i} already performed")
        val initialVersion = getTableInitialVersion(spark, tableNames(i))
        val latestTableVersion = getTableVersion(spark, tableNames(i))
        val updatedTableStates = Seq(TableStates(i, tableNames(i), initialVersion, latestTableVersion)).toDF()
        updateTableStates(spark, updatedTableStates)
      }
      else {
        runAndRegisterQuery(spark, tableNames, transactions(i), i)
      }
      if (i == (transactions.indices.length - 1)) {
        for (j <- tableNames.indices) {
          createView(spark, tableNames(j))
        }
      }
    }
  }

  def runAndRegisterQuery(spark: SparkSession, tableNames: Array[String], transaction: String, i: Int): Unit = {
    import spark.implicits._
    val initialVersion = getTableVersion(spark, tableNames(i))
    spark.sql(transaction)
    print(s"transaction s'${i} committed")
    val latestVersion = getTableVersion(spark, tableNames(i))
    val updatedTableStates = Seq(TableStates(i, tableNames(i), initialVersion, latestVersion)).toDF()
    updateTableStates(spark, updatedTableStates)
  }
}
