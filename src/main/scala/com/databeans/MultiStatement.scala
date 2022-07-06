package com.databeans
import org.apache.spark.sql.SparkSession
import com.databeans.MultiStatementUtils._

object MultiStatement {

  def multiStatementTransaction(spark: SparkSession, transactions: Array[String], tableNames: Array[String]): Unit = {
    //val tableStates = createUniqueTableName(tableNames)
    beginTransaction(spark, transactions, tableNames, "tableStates")
  }
}

