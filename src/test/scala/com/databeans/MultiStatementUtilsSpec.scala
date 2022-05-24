package com.databeans

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import org.apache.spark.sql.test.SharedSparkSession
import com.databeans.MultiStatementUtils._
import org.apache.spark.sql.functions.col


case class Data(value: Long, keys: Long, option: Long)

class MultiStatementUtilsSpec extends QueryTest
  with SharedSparkSession
  with DeltaExtendedSparkSession {
  test("beginTransaction should run multiple non-failing SQL queries"){
    val s = spark
    import s.implicits._

    val data = Seq(1, 5).toDF().withColumn("keys", col("value") * 2).withColumn("option", col("value") * 3)
    data.write.mode("overwrite").format("delta").saveAsTable("my_fake_tab")
    val updatesData = Seq(9, 5).toDF().withColumn("keys", col("value") * 2).withColumn("option", col("value") * 3)
    updatesData.write.mode("overwrite").format("delta").saveAsTable("updates")

    val deleteQuery = "DELETE FROM updates WHERE value = 5"
    val updateQuery = "UPDATE my_fake_tab SET value = 4 WHERE value = 1"



    beginTransaction(spark, Array(deleteQuery, updateQuery), Array("updates", "my_fake_tab"), "tableStates")
    Thread.sleep(5000)

    val updateResult = spark.sql("select * from updates_view")
    val expectedUpdateResult = Seq(Data(9, 18, 27)).toDF()

    val myFakeTabResult = spark.sql("select * from my_fake_tab_view")
    val expectedMyFakeTabResult = Seq(Data(4, 2, 3), Data(5, 10, 15)).toDF()

    assert(updateResult.collect() sameElements expectedUpdateResult.collect())
    assert(myFakeTabResult.collect() sameElements expectedMyFakeTabResult.collect())
  }
}

