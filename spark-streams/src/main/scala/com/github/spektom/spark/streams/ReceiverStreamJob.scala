package com.github.spektom.spark.streams

import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{NumInputDStream, StreamingContext}

object ReceiverStreamJob {

  def main(args: Array[String]): Unit = {
    new Application {
      override def createStream(ssc: StreamingContext): InputDStream[_] = {
        new NumInputDStream(ssc)
      }
    }.run()
  }
}
