package com.databeans

import org.apache.spark.sql.SparkSession
import com.databeans.MultiStatementUtils._

object MultiStatement {
  def multiStatementTransaction(spark: SparkSession, transactions: Array[String], tableNames: Array[String]): Unit = {
    import spark.implicits._
    val biggestPerformedQueryId = getTransactionId(spark)
    if (biggestPerformedQueryId == -1) {
      beginTransaction(spark, transactions, tableNames)
    }
    else {
      rerunTransactions(spark, transactions, tableNames, biggestPerformedQueryId)
    }
  }
}

