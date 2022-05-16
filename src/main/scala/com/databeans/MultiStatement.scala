package com.databeans

import org.apache.spark.sql.SparkSession
import com.databeans.MultiStatementUtils._

object MultiStatement {
  def multiStatementTransaction(spark: SparkSession, transactions: Array[String], tableNames: Array[String], rerun: Boolean): Unit = {
    if (!rerun) beginTransaction(spark, transactions, tableNames)
    else {
      rerunQueries(spark, transactions, tableNames)
    }
  }
  //todo : change tableStates schema to include transaction id, like that we can take into account multiple queries on one table.
}

