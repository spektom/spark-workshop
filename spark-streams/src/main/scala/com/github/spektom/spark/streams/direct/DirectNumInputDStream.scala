package com.github.spektom.spark.streams.direct

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.util.Random

class DirectNumInputDStream(ssc_ : StreamingContext) extends InputDStream[Int](ssc_) {

  override def start(): Unit = {}

  override def stop(): Unit = {}

  override def compute(validTime: Time): Option[RDD[Int]] = {
    Some(new NumRDD(ssc_.sparkContext, 1000))
  }
}

class NumPartition(val idx: Int) extends Partition {
  override def index: Int = idx
}

class NumRDD(@transient val _sc: SparkContext, val numsCount: Int) extends RDD[Int](_sc, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[Int] = {
    0.to(numsCount).map(_ => Random.nextInt(10)).toIterator
  }

  override protected def getPartitions: Array[Partition] = {
    Array(new NumPartition(0))
  }
}
