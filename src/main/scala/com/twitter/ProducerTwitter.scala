package com.twitter


import com.typesafe.config.ConfigFactory
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory
import twitter4j.Status

import java.io._
import java.time.LocalDateTime
import java.util.Properties
import scala.util.Try

object ProducerTwitter {

  val logger = LoggerFactory.getLogger(this.getClass)

  case class Tweet(tweetId:Long,
                   user: String,
                   isRetweet: Boolean,
                   replyUserId: Long,
                   quotedStatusId: Long,
                   countryCode: String,
                   source: String,
                   text: String,
                   language: String,
                   created: LocalDateTime
                  )

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
      .setAppName("TwitterProducer")
      .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val stream = TwitterUtils.createStream(ssc, None, Array("iPhone"))

    stream.foreachRDD((rdd,ts) => {
      if (!rdd.partitions.isEmpty) {
        rdd.repartition(2)
        rdd.zipWithIndex().foreachPartition(it=>sendToKafka(generateJson(it.toArray)))
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(new FileOutputStream(f,true))
    try { op(p) } finally { p.close() }
  }

  def generateJson (tweets:Array[(Status,Long)]):Array[String] = {
    implicit val tweetDecoder: Decoder[Tweet] = deriveDecoder
    implicit val tweetEncoder: Encoder[Tweet] = deriveEncoder

    tweets.map( tweet => {
      val tweetStatus = tweet._1
      val tweetCountryCode = Try(tweetStatus.getPlace.getCountryCode).toOption.getOrElse("none")
      val tweetObj = Tweet(
        tweetStatus.getId,
        tweetStatus.getUser.getName,
        tweetStatus.isRetweet,
        tweetStatus.getInReplyToUserId,
        tweetStatus.getQuotedStatusId,
        tweetCountryCode,
        tweetStatus.getSource,
        tweetStatus.getText,
        tweetStatus.getLang,
        LocalDateTime.now()
      )
      tweetObj.asJson.noSpaces
    })
  }

  def sendToKafka(tweets:Array[String]) = {
    val props:Properties = new Properties()
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks","all")
    val producer = new KafkaProducer[String, String](props)
    val topic = "tweets_topic"
    try {
      tweets.foreach(tweet => {
        val record = new ProducerRecord[String, String](topic, toString, tweet)
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

