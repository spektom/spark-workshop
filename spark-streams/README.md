spark-streams
==============

This project contains to samples for defining custom Spark Streaming input sources:

 * Receiver-based Input Stream
 * Direct Input Stream 
 
## Running

### Receiver-based Spark streaming job

      mvn scala:run -DmainClass=com.github.spektom.spark.streams.receiver.Job
      
### Direct approach Spark streaming job

      mvn scala:run -DmainClass=com.github.spektom.spark.streams.direct.Job
