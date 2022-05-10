package com.databeans

import org.apache.spark.sql.SparkSession
import com.databeans.MultiStatementUtils._

object MultiStatement {
  def beginTransaction(spark: SparkSession, transactions: Array[String], tableNames: Array[String]): Unit = {
    import spark.implicits._

    createTableStates(spark)
    for (i <- transactions.indices) {
      createView(spark, tableNames(i))
      val initialVersion = getTableVersion(spark, tableNames(i))
      spark.sql(transactions(i))
      val latestVersion = getTableVersion(spark, tableNames(i))
      val updatedTableStates = Seq(TableStates(tableNames(i), initialVersion, latestVersion)).toDF()
      if (latestVersion >= initialVersion) {
        updateTableStates(spark, updatedTableStates)
      }
      if (i == transactions.indices.end) {
        for (j <- tableNames.indices) {
          print("we is about to create the views nigga")
          createView(spark, tableNames(j))
        }
      }
    }
  }
}

