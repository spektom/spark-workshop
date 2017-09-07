package com.github.spektom.spark.streams.receiver

import com.github.spektom.spark.streams.Application
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

object Job {

  def main(args: Array[String]): Unit = {
    new Application {
      override def createStream(ssc: StreamingContext): InputDStream[_] = {
        new NumInputDStream(ssc)
      }
    }.run()
  }
}
