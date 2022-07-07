package com.databeans

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, max, min}
import scala.util.Try
import scala.util.control.Breaks

case class TableStates(transaction_id: Int, tableName: String, initialVersion: Long, isCommitted: Boolean)

object MultiStatementUtils {

  def createUniqueTableName(tableNames: Array[String]): String = {
    val uniqueString = "tableStatesOf_" + tableNames.distinct.mkString("_")
    uniqueString
  }

  def createTableStates(spark: SparkSession, tableStates: String): Unit = {
    import spark.implicits._
    val emptyConf: Seq[TableStates] = Seq()
    emptyConf.toDF().write.format("delta").mode("overwrite").saveAsTable(tableStates)
  }

  def getCurrentTableVersion(spark: SparkSession, tableName: String): Long = {
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
        "initialVersion" -> "updatedTableStates.initialVersion",
        "isCommitted" -> "updatedTableStates.isCommitted"))
      .whenNotMatched()
      .insertAll()
      .execute()
  }

  def getInitialTableVersion(spark: SparkSession, tableStates: String, tableNames: Array[String], i: Int): Long = {
    import spark.implicits._
    spark.read.format("delta").table(tableStates).where(col("tableName") === tableNames(i)).select(min(col("initialVersion"))).as[Long].head()
  }

  def getVersionBeforeQuery(spark: SparkSession, tableStates: String, i: Int): Long = {
    import spark.implicits._
    spark.read.format("delta").table(tableStates).where(col("transaction_id") === i).select(col("initialVersion")).as[Long].head()
  }

  def createViews(spark: SparkSession, tableNames: Array[String]): Unit = {
    val distinctTables = tableNames.distinct
    for (i <- distinctTables.indices) {
      spark.read.format("delta").option("versionAsOf", getCurrentTableVersion(spark, distinctTables(i))).table(distinctTables(i)).createOrReplaceTempView(distinctTables(i) + "_view")
    }
  }

  def runAndRegisterQuery(spark: SparkSession, tableNames: Array[String], transaction: String, tableStates: String, i: Int): Unit = {
    import spark.implicits._

    val initialVersion = getCurrentTableVersion(spark, tableNames(i))
    val updatedTableStates = Seq(TableStates(i, tableNames(i), initialVersion, false)).toDF()
    updatedTableStates.write.format("delta").mode("append").saveAsTable(tableStates)
    spark.sql(transaction)
    print(s"query ${i} performed ")
    val latestVersion = getCurrentTableVersion(spark, tableNames(i))
    val commitToTableStates = Seq(TableStates(i, tableNames(i), latestVersion, true)).toDF()
    updateTableStates(spark, commitToTableStates, tableStates)
  }

  def isCommitted(spark: SparkSession, tableStates: String, transaction_id: Int, tableNames: Array[String]): Boolean = {
    import spark.implicits._

    val isRegistered = Try {
      spark.read.format("delta").table(tableStates).select("isCommitted").where(col("transaction_id") === transaction_id).as[Boolean].head()
    }.getOrElse(-1L)

    if (isRegistered == true) {
      true
    }
    else if (isRegistered == false) {
      if (getCurrentTableVersion(spark, tableNames(transaction_id)) - getVersionBeforeQuery(spark, tableStates, transaction_id) == 1) {
        true
      }
      else {
        false
      }
    }
    else {
      false
    }
  }

  def beginTransaction(spark: SparkSession, transactions: Array[String], tableNames: Array[String], tableStates: String): Unit = {
    import spark.implicits._

    createViews(spark, tableNames)
    createTableStates(spark, tableStates)
    val loop = new Breaks
    loop.breakable {
      for (j <- transactions.indices) {
        try {
          runAndRegisterQuery(spark, tableNames, transactions(j), tableStates, j)
        } catch {
          case _: Throwable =>
            if (isCommitted(spark, tableStates, j, tableNames)) {
              val affectedTables = tableNames.slice(0, j + 1).distinct
              for (i <- affectedTables.indices) {
                spark.sql(s"RESTORE TABLE ${affectedTables(i)} TO VERSION AS OF ${getInitialTableVersion(spark, tableStates, affectedTables, i)} ")
                print(s"${affectedTables(i)} rolled back ")
              }
              spark.sql(s"drop table ${tableStates}")
              loop.break
            }
            else {
              val affectedTables = tableNames.slice(0, j).distinct
              for (i <- affectedTables.indices) {
                spark.sql(s"RESTORE TABLE ${affectedTables(i)} TO VERSION AS OF ${getInitialTableVersion(spark, tableStates, affectedTables, i)} ")
                print(s"${affectedTables(i)} rolled back ")
              }
              spark.sql(s"drop table ${tableStates}")
              loop.break
            }
        }
        if (j == (transactions.indices.length - 1)) {
          createViews(spark, tableNames)
          spark.sql(s"drop table ${tableStates}")
        }
      }
    }
  }

  def extractTableNamesFromQuery(spark: SparkSession, query: String): String = {
    import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
    val logicalPlan = spark.sessionState.sqlParser.parsePlan(query)
    val tablesInQuery = logicalPlan.collect { case r: UnresolvedRelation => r.tableName }
    if (tablesInQuery.nonEmpty) {
      tablesInQuery.head
    }
    else {
      query.split(" ")(2)
    }
  }

  def createTableNames(spark: SparkSession, transactions: Array[String]): Array[String] = {
    val tableNames = Array.tabulate(transactions.length)(t => extractTableNamesFromQuery(spark, transactions(t)))
    tableNames
  }
}
