package com.github.spektom.spark.streams.direct

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.util.Random

class NumRDD(@transient val _sc: SparkContext, val numsCount: Int) extends RDD[Int](_sc, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[Int] = {
    logWarning(s"Computing partition ${split.index} of NumRDD")
    0.to(numsCount).map(_ => Random.nextInt(10)).toIterator
  }

  override protected def getPartitions: Array[Partition] = {
    Array(new Partition {
      override def index: Int = 0
    })
  }
}