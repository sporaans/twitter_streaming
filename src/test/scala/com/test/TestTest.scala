package com.test

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TestTest extends AnyFunSuite with Matchers{

  val spark: SparkSession = SparkSession.builder
    .appName("TestApp")
    .config("spark.master", "local")
    .config("spark.default.parallelism", "1")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate

  test("test") {
    import spark.implicits._

    val df = Seq(("a", 1), ("b", 1), ("c", 3)).toDF()

    df.show()

    df.count() shouldBe 3
  }

}
