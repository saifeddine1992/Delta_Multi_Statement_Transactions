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

    val data = Seq(1, 5).toDF().withColumn("keys", col("value") * 2).withColumn("option", col("value") * 3)
    data.write.mode("overwrite").format("delta").saveAsTable("my_fake_tab")
    val updatesData = Seq(9, 5).toDF().withColumn("keys", col("value") * 2).withColumn("option", col("value") * 3)
    updatesData.write.mode("overwrite").format("delta").saveAsTable("updates")

    val deleteQuery = "DELETE FROM updates WHERE value = 5"
    val updateQuery = "UPDATE my_fake_tab SET value = 6 WHERE value = 1"

    beginTransaction(spark, Array(deleteQuery, updateQuery), Array("updates", "my_fake_tab"))
    Thread.sleep(5000)

    val result = spark.sql("select * from updates")
    val expectedResult = Seq(Data(5, 10, 15), Data(98, 196, 294), Data(102, 204, 306), Data(100, 200, 300), Data(1, 2, 3)).toDF()

    //assert(result.except(expectedResult).isEmpty)
    result.show()
  }
}
