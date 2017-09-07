package com.github.spektom.spark.streams

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

abstract class Application {

  def createStream(ssc: StreamingContext): InputDStream[_]

  def run(): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Streaming Application")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    val stream = createStream(ssc)
    stream.map(num => (num, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
