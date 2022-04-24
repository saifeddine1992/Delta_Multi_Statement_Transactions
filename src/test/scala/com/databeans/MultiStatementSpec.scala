package com.databeans
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import org.apache.spark.sql.test.SharedSparkSession
import com.databeans.MultiStatement._
import org.apache.spark.sql.functions.col

class MultiStatementSpec extends QueryTest
  with SharedSparkSession
  with DeltaExtendedSparkSession {
 test("beginTransaction should run multiple SQL queries") {
   val s = spark
   import s.implicits._
   val data = Seq(1 , 100).toDF().withColumn("keys", col("value") * 2).withColumn("option", col("value") * 3)
   data.write.format("delta").saveAsTable("my_fake_tab")

   val updatesData = Seq(98 , 102).toDF().withColumn("keys", col("value") * 2).withColumn("option", col("value") * 3)
   updatesData.write.mode("append").partitionBy("option").format("delta").saveAsTable("updates")

   val mergeQuery = """
                  |MERGE INTO my_fake_tab
                  |USING updates
                  |ON my_fake_tab.value = updates.value
                  |WHEN MATCHED THEN
                  |  UPDATE SET my_fake_tab.value = updates.value
                  |WHEN NOT MATCHED
                  |  THEN INSERT (value, keys, option) VALUES (value, keys, option)
                  |""".stripMargin

   val scanQuery = "select * from my_fake_tab"

   beginTransaction(spark, Array(mergeQuery,  scanQuery))
   Thread.sleep(5000)
   spark.read.format("delta").table("my_fake_tab").show()
 }
}
