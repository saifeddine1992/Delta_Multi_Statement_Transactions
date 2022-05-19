package com.databeans
import scala.util.control._
import org.apache.spark.sql.SparkSession
import com.databeans.MultiStatementUtils._

object MultiStatement {
  def multiStatementTransaction(spark: SparkSession, transactions: Array[String], tableNames: Array[String]): Unit = {
    import spark.implicits._
    val loop = new Breaks
    loop.breakable {
      for (i <- transactions.indices) {
        val initialVersion = getTableInitialVersion(spark, tableNames(i), i)
        val latestVersion = getTableVersion(spark, tableNames(i))
        val incrementInVersion = latestVersion - initialVersion == 1

        if ((isCommitted(spark, i) == -1L) & (i == 0)){
          beginTransaction(spark, transactions, tableNames)
          loop.break
        }
        else if (isCommitted(spark, i) == false & incrementInVersion){
          val commitToTableStates = Seq(TableStates(i, tableNames(i), initialVersion, latestVersion, true)).toDF()
          updateTableStates(spark, commitToTableStates)
        }
        else if (((isCommitted(spark, i) == -1) & (i > 0)) || (isCommitted(spark, i) == true)){
          val latestTransaction_id = getTransactionId(spark)
          rerunTransactions(spark, transactions, tableNames, latestTransaction_id)
          loop.break
        }
      }
    }
  }
}

