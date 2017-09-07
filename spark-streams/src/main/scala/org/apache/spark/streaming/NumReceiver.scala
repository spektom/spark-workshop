package org.apache.spark.streaming

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.util.Random

class NumReceiver(storageLevel: StorageLevel) extends Receiver[Int](storageLevel)
  with Logging {

  val running = new AtomicBoolean(true)

  override def onStart(): Unit = {
    logWarning("Start receiving data")
    new Thread("NumReceiver") {
      override def run(): Unit = {
        while (running.get) {
          // Receive number from 0 to 10
          store(Random.nextInt(10))
          Thread.sleep(50)
        }
      }
    }.start()
  }

  override def onStop(): Unit = {
    logWarning("Stop receiving data")
    running.set(false)
  }
}
