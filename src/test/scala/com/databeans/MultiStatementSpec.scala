package com.databeans

import com.databeans.MultiStatement._
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession

class MultiStatementSpec extends QueryTest
  with SharedSparkSession
  with DeltaExtendedSparkSession {

  test("multiStatementTransaction should rerun multiple non-failing SQL queries"){
    val s = spark
    import s.implicits._

    val data = Seq(1, 5).toDF().withColumn("keys", col("value") * 2).withColumn("option", col("value") * 3)
    data.write.mode("overwrite").format("delta").saveAsTable("my_fake_tab")
    val updatesData = Seq(9, 5).toDF().withColumn("keys", col("value") * 2).withColumn("option", col("value") * 3)
    updatesData.write.mode("overwrite").format("delta").saveAsTable("updates")

    val secondDeleteQuery = "DELETE FROM my_fake_tab WHERE value = 5"
    val deleteQuery = "DELETE FROM updates WHERE value = 5"
    val updateQuery = "UPDATE updates SET value = 6 WHERE value = 9"
    val insertQuery = "INSERT INTO  my_fake_tab VALUES (0, 0, 0)"

    multiStatementTransaction(spark, Array(deleteQuery, secondDeleteQuery, updateQuery, insertQuery), Array("updates", "my_fake_tab", "updates", "my_fake_tab"))
    Thread.sleep(5000)
    multiStatementTransaction(spark, Array(deleteQuery, secondDeleteQuery, updateQuery, insertQuery), Array("updates", "my_fake_tab", "updates", "my_fake_tab"))

    val result = spark.sql("select * from updates_view")
    val expectedResult = Seq(Data(6, 18, 27)).toDF()
    val fakeTabResult = spark.sql("select * from my_fake_tab_view")
    val expectedFakeTabResult = Seq(Data(1, 2, 3), Data(0, 0, 0)).toDF()
    assert(result.collect() sameElements expectedResult.collect())
    assert(fakeTabResult.except(expectedFakeTabResult).isEmpty)
  }
}
