package com.github.spektom.spark.streams.direct

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.scheduler.StreamInputInfo
import org.apache.spark.streaming.{StreamingContext, Time}

class DirectNumInputDStream(ssc_ : StreamingContext) extends InputDStream[Int](ssc_)
    with Logging {

  override def start(): Unit = {}

  override def stop(): Unit = {}

  override def compute(validTime: Time): Option[RDD[Int]] = {
    logWarning(s"Computing next RDD for ${validTime}")
    val rdd = new NumRDD(ssc_.sparkContext, 1000)
    //ssc_.scheduler.inputInfoTracker.reportInfo(validTime, StreamInputInfo(id, rdd.count))
    Some(rdd)
  }
}
