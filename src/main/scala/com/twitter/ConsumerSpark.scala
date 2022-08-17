package com.twitter

import com.datastax.driver.core.Cluster
import org.apache.spark.sql.functions.{col, from_json, lit, regexp_extract}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ConsumerSpark {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("TwitterConsumer")
      .getOrCreate()

    spark.conf.set(s"spark.cassandra.connection.host", "127.0.0.1")
    spark.conf.set(s"spark.cassandra.connection.port", "9042")
    spark.conf.set(s"spark.cassandra.auth.username", "cassandra")
    spark.conf.set(s"spark.cassandra.auth.password", "cassandra")
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "tweets_topic")
      .option("startingOffsets", "earliest") // From starting
      .option("failOnDataLoss", "false")
      .load()

    df.printSchema()

    val schema = new StructType()
      .add("tweetId", LongType, false)
      .add("user", StringType, true)
      .add("isRetweet", BooleanType, true)
      .add("replyUserId", LongType, true)
      .add("quotedStatusId", LongType, true)
      .add("countryCode", StringType, true)
      .add("source", StringType, true)
      .add("text", StringType, true)
      .add("language", StringType, true)
      .add("created",TimestampType,false)

    val data_cc = df.selectExpr("CAST(value AS STRING)")
      .select(col("value"))
      .withColumn("value2",from_json(col("value"),schema))
      .select(col("value2.*"))

    val snakeColNames = Seq("tweet_id", "user", "is_retweeted", "reply_user_id", "quoted_status_id", "country_code",
      "source" , "tweet_text", "language", "created")

    val data = data_cc.toDF(snakeColNames: _*)

    data.printSchema()

    val cluster = Cluster.builder()
      .withCredentials("cassandra","cassandra")
      .addContactPoint("127.0.0.1")
      .withoutMetrics()
      .build()

    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS analytics WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("DROP TABLE IF EXISTS analytics.twitter_data;")
    session.execute("DROP TABLE IF EXISTS analytics.tweet_types;")
    session.execute("DROP TABLE IF EXISTS analytics.platform_types;")
    session.execute("DROP TABLE IF EXISTS analytics.languages;")
    session.execute("CREATE TABLE IF NOT EXISTS analytics.tweet_types (created timestamp, type text, PRIMARY KEY (type, created));")
    session.execute("CREATE TABLE IF NOT EXISTS analytics.platform_types (created timestamp, platform text, PRIMARY KEY (platform, created));")
    session.execute("CREATE TABLE IF NOT EXISTS analytics.languages (created timestamp, language text, PRIMARY KEY (language, created));")
    session.execute("""CREATE TABLE IF NOT EXISTS analytics.twitter_data
                   (tweet_id bigint PRIMARY KEY,
                   user text,
                   is_retweeted boolean,
                   reply_user_id bigint,
                   quoted_status_id bigint,
                   country_code text,
                   source text,
                   tweet_text text,
                   language text,
                   created timestamp);
                   """)

    /**
     *uncomment below code if you want to write it to console for testing.
     */
//        val query = data.writeStream
//          .format("console")
//          .outputMode("append")
//          .start()
//          .awaitTermination()

    val queryMain = writeMainTable(data)
    val queryTweetType = writeTweetType(data)
    val queryPlatformTypes = writePlatformType(data)
    val queryLanguages = writeLanguages(data)
      .awaitTermination()
  }

  def writeMainTable(input: DataFrame): StreamingQuery = {
    input.writeStream
      .option("checkpointLocation", "/tmp/check_point/")
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "analytics")
      .option("table", "twitter_data")
      .start()
  }

  def writeTweetType(input: DataFrame): StreamingQuery = {
    val retweets = input.filter(col("is_retweeted")===true)
      .select(col("created"))
      .withColumn("type",lit("Retweet"))
    val replies = input.filter(col("reply_user_id") =!= -1)
      .select(col("created"))
      .withColumn("type",lit("Reply"))
    val quotes = input.filter(col("quoted_status_id") =!= -1)
      .select(col("created"))
      .withColumn("type",lit("Quote"))
    val justTweets = input.filter(col("is_retweeted") === false ||
                                  col("reply_user_id") === -1 ||
                                  col("quoted_status_id") === -1)
      .select(col("created"))
      .withColumn("type", lit("Regular tweet"))

    val dfs = Seq(retweets, replies, quotes, justTweets)
    val data = dfs.reduce(_ union _)
//    val data = data_un
//      .withWatermark("created","10 seconds")
//      .groupBy("created","type")
//      .count()
    data.writeStream
      .option("checkpointLocation", "/tmp/check_point3/")
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "analytics")
      .option("table", "tweet_types")
      .start()
  }

  def writePlatformType(input: DataFrame):StreamingQuery = {
    input.withColumn("platform", regexp_extract(col("source"),"<a href.*?>(.*)</a>",1))
      .select(col("created"),col("platform"))
      .writeStream
      .option("checkpointLocation", "/tmp/check_point4/")
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "analytics")
      .option("table", "platform_types")
      .start()
  }

  def writeLanguages(input: DataFrame):StreamingQuery = {
    input.select(col("created"),col("language"))
      .writeStream
      .option("checkpointLocation", "/tmp/check_point5/")
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "analytics")
      .option("table", "languages")
      .start()
  }

}
