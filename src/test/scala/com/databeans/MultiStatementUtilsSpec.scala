package com.databeans

import org.apache.spark.sql.QueryTest
import com.databeans.MultiStatementUtils._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession


case class Data(value: Long, keys: Long, option: Long)

class MultiStatementUtilsSpec extends QueryTest
  with SharedSparkSession with DeltaExtendedSparkSession {


  test("beginTransaction should run multiple non-failing SQL queries"){
    val s = spark
    import s.implicits._

    sql("create database user_db")
    sql("use user_db")

    val data = Seq(1, 5).toDF().withColumn("keys", col("value") * 2).withColumn("option", col("value") * 3)
    data.repartition(1).write.mode("append").format("delta").saveAsTable("my_fake_table")
    val updatesData = Seq(9, 5).toDF().withColumn("keys", col("value") * 2).withColumn("option", col("value") * 3)
    updatesData.repartition(1).write.mode("append").format("delta").saveAsTable("update")

    val deleteQuery = "DELETE FROM update WHERE value = 5"
    val updateQuery = "UPDATE my_fake_table SET value = 4 WHERE value = 1"

    beginTransaction(spark, Array(deleteQuery, updateQuery), Array("update", "my_fake_table"), "tableStates")
    Thread.sleep(5000)

    val updateResult = spark.sql("select * from update_view")
    val expectedUpdateResult = Seq(Data(9, 18, 27)).toDF()

    val myFakeTabResult = spark.sql("select * from my_fake_table_view")
    val expectedMyFakeTabResult = Seq(Data(4, 2, 3), Data(5, 10, 15)).toDF()

    assert(updateResult.collect() sameElements expectedUpdateResult.collect())
    assert(myFakeTabResult.except(expectedMyFakeTabResult).isEmpty)
  }

  test("extractTableNamesFromQuery should extract tableNames SQL queries"){
    val s = spark

    val deleteQuery = "DELETE FROM updates WHERE value = 5"
    val updateQuery = "UPDATE my_fake_tab SET value = 4 WHERE value = 1"

    val result = extractTableNames(spark, Array(deleteQuery, updateQuery))
    val expectedUpdateResult = Array("updates", "my_fake_tab")

    assert(result sameElements expectedUpdateResult)
  }

  test("multiStatementTransaction should run multiple failing SQL queries and restore tables"){
    val s = spark
    import s.implicits._

    val data = Seq(1, 5).toDF().withColumn("keys", col("value") * 2).withColumn("option", col("value") * 3)
    data.write.mode("overwrite").format("delta").saveAsTable("my_fake_tab")
    val updatesData = Seq(9, 5).toDF().withColumn("keys", col("value") * 2).withColumn("option", col("value") * 3)
    updatesData.write.mode("overwrite").format("delta").saveAsTable("updates")

    val secondDeleteQuery = "DELETE FROM my_fake_tab WHERE value = 5"
    val deleteQuery = "DELETE FROM updates WHERE value = 5"
    val updateQuery = "UPDATE updates SET value = 6 WHERE value = 9"
    val insertQuery = "INSERT INTO  my_fake_tab VALUES (0, 0, 0, 0)"

    beginTransaction(spark, Array(deleteQuery, secondDeleteQuery, updateQuery, insertQuery), Array("updates", "my_fake_tab", "updates", "my_fake_tab"), "tableStates")
    Thread.sleep(5000)

    val result = spark.sql("select * from updates_view")
    val expectedResult = Seq(Data(9, 18, 27), Data(5, 10, 15)).toDF()
    val fakeTabResult = spark.sql("select * from my_fake_tab_view")
    val expectedFakeTabResult = Seq(Data(1, 2, 3), Data(5, 10, 15)).toDF()
    assert(result.except(expectedResult).isEmpty)
    assert(fakeTabResult.except(expectedFakeTabResult).isEmpty)
  }
}

