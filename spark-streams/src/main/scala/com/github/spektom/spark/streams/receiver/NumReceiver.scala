package com.github.spektom.spark.streams.receiver

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.util.Random

class NumReceiver(storageLevel: StorageLevel) extends Receiver[Int](storageLevel)
  with Logging with Runnable {

  val running = new AtomicBoolean(true)

  override def onStart(): Unit = {
    logInfo("Start receiving data")
    val executor = Executors.newFixedThreadPool(1)
    executor.submit(this)
    executor.shutdown()
  }

  override def onStop(): Unit = {
    logInfo("Stop receiving data")
    running.set(false)
  }

  override def run() = {
    while (running.get) {
      // Receive number from 0 to 10
      store(Random.nextInt(10))
      Thread.sleep(50)
    }
  }
}
