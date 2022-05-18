package com.databeans

import org.apache.spark.sql.SparkSession
import com.databeans.MultiStatementUtils._

object MultiStatement {
  def multiStatementTransaction(spark: SparkSession, transactions: Array[String], tableNames: Array[String], rerun: Boolean = false): Unit = {
    if (!rerun) beginTransaction(spark, transactions, tableNames)
    else {
      rerunQueries(spark, transactions, tableNames)
    }
  }
}

