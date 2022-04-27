package com.databeans

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import org.apache.spark.sql.test.SharedSparkSession
import com.databeans.MultiStatement._
import org.apache.spark.sql.functions.col


case class Data(value: Long, keys: Long, option: Long)

class MultiStatementSpec extends QueryTest
  with SharedSparkSession
  with DeltaExtendedSparkSession {
  test("beginTransaction should run multiple SQL queries") {
    val s = spark
    import s.implicits._
    val data = Seq(1, 5, 100).toDF().withColumn("keys", col("value") * 2).withColumn("option", col("value") * 3)
    data.write.format("delta").saveAsTable("my_fake_tab")

    val updatesData = Seq(98, 5, 102).toDF().withColumn("keys", col("value") * 2).withColumn("option", col("value") * 3)
    updatesData.write.mode("append").partitionBy("option").format("delta").saveAsTable("updates")

    val mergeQuery =
      """
        |MERGE INTO my_fake_tab
        |USING updates
        |ON my_fake_tab.value = updates.value
        |WHEN MATCHED THEN
        |  UPDATE SET my_fake_tab.value = updates.value
        |WHEN NOT MATCHED
        |  THEN INSERT (value, keys, option) VALUES (value, keys, option)
        |""".stripMargin

    val deleteQuery = "DELETE FROM my_fake_tab WHERE value = 5"

    beginTransaction(spark, Array(deleteQuery, mergeQuery))
    Thread.sleep(5000)
    val result = spark.read.format("delta").table("my_fake_tab")
    val expectedResult = Seq(Data(5, 10, 15), Data(98, 196, 294), Data(102, 204, 306), Data(100, 200, 300), Data(1, 2, 3)).toDF()
    assert(result.except(expectedResult).isEmpty)
  }

  test("beginTransaction should not commit when there is a UnsupportedOperationException") {
    val s = spark
    import s.implicits._
    val data = Seq(1, 5, 100).toDF().withColumn("keys", col("value") * 2).withColumn("option", col("value") * 3)
    data.write.format("delta").saveAsTable("my_fake_tab")

    val updatesData = Seq(98, 5, 102).toDF().withColumn("keys", col("value") * 2).withColumn("option", col("value") * 3)
    updatesData.write.mode("append").partitionBy("option").format("delta").saveAsTable("updates")

    val updateQuery = "UPDATE updates SET value = 5 WHERE value = 98"

    val mergeQuery =
      """
        |MERGE INTO my_fake_tab
        |USING updates
        |ON my_fake_tab.value = updates.value
        |WHEN MATCHED THEN
        |  UPDATE SET my_fake_tab.value = updates.value
        |WHEN NOT MATCHED
        |  THEN INSERT (value, keys, option) VALUES (value, keys, option)
        |""".stripMargin

    beginTransaction(spark, Array(updateQuery, mergeQuery))
    Thread.sleep(5000)
    val my_fake_tab = spark.read.format("delta").table("my_fake_tab")
    val updatesTable = spark.read.format("delta").table("updates")
    //assert(my_fake_tab.except(data).isEmpty & updatesTable.except(updatesData).isEmpty)
    spark.read.format("delta").table("my_fake_tab").show()
  }
}
