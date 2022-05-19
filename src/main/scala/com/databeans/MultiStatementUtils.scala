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

  def getTableInitialVersion(spark: SparkSession, tableName: String, i: Int): Long = {
    import spark.implicits._
    import org.apache.spark.sql.functions.col
    Try {
      spark.read.format("delta").table("tableStates").select("initialVersion").where(col("transaction_id") === i).as[Long].head()
    }.getOrElse(0L)
  }

  def getTransactionId(spark: SparkSession): Long = {
    import spark.implicits._
    import org.apache.spark.sql.functions.col
    Try {
      spark.read.format("delta").table("tableStates").select(max(col("transaction_id"))).as[Long].head()
    }.getOrElse(-1L)
  }

  def createViews(spark: SparkSession, tableNames: Array[String]): Unit = {
    for (i <- tableNames.indices) {
      spark.read.format("delta").table(tableNames(i)).createOrReplaceTempView(tableNames(i) + "_view")
    }
  }

  def runAndRegisterQuery(spark: SparkSession, tableNames: Array[String], transaction: String, i: Int): Unit = {
    import spark.implicits._
    val initialVersion = getTableVersion(spark, tableNames(i))
    spark.sql(transaction)
    print(s"query ${i} performed ")
    val latestVersion = getTableVersion(spark, tableNames(i))
    val updatedTableStates = Seq(TableStates(i, tableNames(i), initialVersion, latestVersion)).toDF()
    updateTableStates(spark, updatedTableStates)
  }

  def beginTransaction(spark: SparkSession, transactions: Array[String], tableNames: Array[String]): Unit = {
    import spark.implicits._

    createViews(spark, tableNames)
    createTableStates(spark)
    for (j <- transactions.indices) {
      runAndRegisterQuery(spark, tableNames, transactions(j), j)
      if (j == (transactions.indices.length - 1)) {
        createViews(spark, tableNames)
        spark.sql("Drop table tableStates")
      }
    }
  }

  def rerunTransactions(spark: SparkSession, transactions: Array[String], tableNames: Array[String], biggestPerformedQueryId: Long): Unit = {
    import spark.implicits._

    for (i <- transactions.indices) {
      if (i > biggestPerformedQueryId) {
        runAndRegisterQuery(spark, tableNames, transactions(i), i)
      }
      if (i == (transactions.indices.length - 1)) {
        createViews(spark, tableNames)
        spark.sql("Drop table tableStates")
      }
    }
  }
}
