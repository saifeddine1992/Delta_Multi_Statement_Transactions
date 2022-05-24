package com.databeans

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, max}
import scala.util.Try

case class TableStates(transaction_id: Int, tableName: String, initialVersion: Long, latestVersion: Long, isCommitted: Boolean)

object MultiStatementUtils {

  def createUniqueTableName(tableNames : Array[String]): String = {
    val uniqueString = "tableStatesOf" + tableNames.distinct.mkString("")
    uniqueString
  }

  def createTableStates(spark: SparkSession, tableStates: String): Unit = {
    import spark.implicits._
    val emptyConf: Seq[TableStates] = Seq()
    emptyConf.toDF().write.format("delta").mode("overwrite").saveAsTable(tableStates)
  }

  def getTableVersion(spark: SparkSession, tableName: String): Long = {
    import io.delta.tables._
    import spark.implicits._
    DeltaTable.forName(spark, tableName).history().select(max(col("version"))).as[Long].head()
  }

  def updateTableStates(spark: SparkSession, updatedTableStates: DataFrame, tableStates: String): Unit = {
    DeltaTable.forName(spark, tableStates).as(tableStates)
      .merge(updatedTableStates.as("updatedTableStates"),
        s"${tableStates}.transaction_id = updatedTableStates.transaction_id")
      .whenMatched()
      .updateExpr(Map(
        "latestVersion" -> "updatedTableStates.latestVersion",
        "isCommitted" -> "updatedTableStates.isCommitted"))
      .whenNotMatched()
      .insertAll()
      .execute()
  }

  def getTableInitialVersion(spark: SparkSession, tableName: String, tableStates: String, i: Int): Long = {
    import spark.implicits._

    Try {
      spark.read.format("delta").table(tableStates).select("initialVersion").where(col("transaction_id") === i).as[Long].head()
    }.getOrElse(0L)
  }

  def getTransactionId(spark: SparkSession, tableStates: String): Long = {
    import spark.implicits._

    Try {
      spark.read.format("delta").table(tableStates).select(max(col("transaction_id"))).as[Long].head()
    }.getOrElse(-1L)
  }

  def isCommitted(spark: SparkSession, tableStates: String, transaction_id: Int): AnyVal = {
    import spark.implicits._

    Try {
      spark.read.format("delta").table(tableStates).select("isCommitted").where(col("transaction_id") === transaction_id).as[Boolean].head()
    }.getOrElse(-1L)
  }

  def createViews(spark: SparkSession, tableNames: Array[String]): Unit = {
    val distinctTables = tableNames.distinct
    for (i <- distinctTables.indices) {
      spark.read.format("delta").option("versionAsOf", getTableVersion(spark, distinctTables(i))).table(distinctTables(i)).createOrReplaceTempView(tableNames(i) + "_view")
    }
  }

  def runAndRegisterQuery(spark: SparkSession, tableNames: Array[String], transaction: String, tableStates: String , i: Int): Unit = {
    import spark.implicits._

    val initialVersion = getTableVersion(spark, tableNames(i))
    val updatedTableStates = Seq(TableStates(i, tableNames(i), initialVersion, -1L, false)).toDF()
    updatedTableStates.write.format("delta").mode("append").saveAsTable(tableStates)
    spark.sql(transaction)
    print(s"query ${i} performed ")
    val latestVersion = getTableVersion(spark, tableNames(i))
    val commitToTableStates = Seq(TableStates(i, tableNames(i), initialVersion, latestVersion, true)).toDF()
    updateTableStates(spark, commitToTableStates, tableStates)
  }

  def beginTransaction(spark: SparkSession, transactions: Array[String], tableNames: Array[String], tableStates: String): Unit = {
    import spark.implicits._

    createViews(spark, tableNames)
    createTableStates(spark, tableStates)
    for (j <- transactions.indices) {
      runAndRegisterQuery(spark, tableNames, transactions(j),tableStates, j)
      if (j == (transactions.indices.length - 1)) {
        createViews(spark, tableNames)
      }
    }
  }

  def rerunTransactions(spark: SparkSession, transactions: Array[String], tableNames: Array[String], tableStates: String, latestPerformedQueryId: Long): Unit = {
    import spark.implicits._

    for (i <- transactions.indices) {
      if (i <= latestPerformedQueryId) {
        print(s"query ${i} already performed ")
      }
      else if (i > latestPerformedQueryId) {
        runAndRegisterQuery(spark, tableNames, transactions(i), tableStates, i)
      }
      if (i == (transactions.indices.length - 1)) {
        createViews(spark, tableNames)
        spark.sql(s"drop table ${tableStates}")
      }
    }
  }
}
