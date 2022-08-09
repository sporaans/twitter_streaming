package com.twitter


import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory
import twitter4j.Status

import java.io._
import java.util.Properties

object ProducerTwitter {

  val logger = LoggerFactory.getLogger(this.getClass)

//    case class CLIParams(hdfsMaster: String = "", filters: Array[String] = Array.empty)

  def main(args:Array[String]): Unit = {

    val config = ConfigFactory.load()
    val consumerKey = config.getString("twitter4j.oauth.consumerKey")
    val consumerSecret = config.getString("twitter4j.oauth.consumerSecret")
    val accessToken = config.getString("twitter4j.oauth.accessToken")
    val accessTokenSecret = config.getString("twitter4j.oauth.accessTokenSecret")


    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf()
      .setAppName("TwitterPopularTags")
      .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val x = ssc.sparkContext.parallelize(Array(1, 2, 3))
    x.foreach(println(_))
    val stream = TwitterUtils.createStream(ssc, None, Array("iPhone"))

    stream.foreachRDD((rdd,ts) =>{
      if (!rdd.partitions.isEmpty) {
        sendToKafka(rdd.collect())
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(new FileOutputStream(f,true))
    try { op(p) } finally { p.close() }
  }

  def sendToKafka(tweets:Array[Status]) = {
    val props:Properties = new Properties()
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks","all")
    val producer = new KafkaProducer[String, String](props)
    val topic = "scala_events"
    try {
      tweets.map(tweet => {
        val message = tweet.getUser.getName
        val record = new ProducerRecord[String, String](topic, toString, message)
        val metadata = producer.send(record)
        printf(s"sent record(key=%s value=%s) " +
          "meta(partition=%d, offset=%d)\n",
          record.key(), record.value(),
          metadata.get().partition(),
          metadata.get().offset())
      })
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      println("Closing..")
      producer.close()
      println("closed")
    }
  }
}

