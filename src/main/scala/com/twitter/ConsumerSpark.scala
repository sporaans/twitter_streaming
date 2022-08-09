package com.twitter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

object ConsumerSpark {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkByExample")
      .getOrCreate()

    spark.conf.set(s"spark.cassandra.connection.host", "127.0.0.1")
    spark.conf.set(s"spark.cassandra.connection.port", "9042")
    spark.conf.set(s"spark.cassandra.auth.username", "cassandra")
    spark.conf.set(s"spark.cassandra.auth.password", "cassandra")
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "scala_events")
      .option("startingOffsets", "earliest") // From starting
      .load()

    df.printSchema()

    val data = df.selectExpr("CAST(value AS STRING)")
      .select(col("value"))


    /**
     *uncomment below code if you want to write it to console for testing.
     */
//        val query = data.writeStream
//          .format("console")
//          .outputMode("append")
//          .start()
//          .awaitTermination()

    val query = data.writeStream
      .option("checkpointLocation", "/tmp/check_point/")
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "analytics")
      .option("table", "test_data")
      .start()
      .awaitTermination()
  }
}
