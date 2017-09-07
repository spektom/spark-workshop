package org.apache.spark.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

class NumInputDStream(_ssc: StreamingContext, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER)
  extends ReceiverInputDStream[Int](_ssc) with Logging {

  override def getReceiver(): Receiver[Int] = {
    logWarning("Creating new receiver")
    new NumReceiver(storageLevel)
  }
}
