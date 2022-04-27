package com.databeans

import org.apache.spark.sql.SparkSession

object MultiStatement {

  /*def revert(spark: SparkSession, tableNames: Array[String]): Unit = {
    import spark.implicits._
    for (i <- tableNames.indices)
    {
      val deltaTable = DeltaTable.forName(spark, tableNames(i))
      deltaTable.restoreToversion(0)
    }
  }*/

 def beginTransaction(spark : SparkSession, transactions: Array[String]): Unit = {
   try
     {
       for (i <- transactions.indices)
       {spark.sql(transactions(i))}
     }
   catch {
     case error: UnsupportedOperationException =>
       print("UnsupportedOperationException")   //revert changes done and give up doing the rest
   }
 }
}
