package com.databeans

import org.apache.spark.sql.SparkSession

object MultiStatement {
 def beginTransaction(spark : SparkSession, transactions: Array[String]): Unit = {
   for (i <- transactions.indices)
   {spark.sql(transactions(i))}
 }
}
