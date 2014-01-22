package pl.elka.pw.sparkseq.util

/**
 * Created by marek on 1/22/14.
 */
object SparkSeqContexProperties {

  def setupContexProperties() ={
    //	System.setProperty("spark.storage.memoryFraction","0.33")
    System.setProperty("spark.executor.memory", "10g")
    //System.setProperty("spark.locality.wait","600")
    //	System.setProperty("spark.rdd.compress", "true")
    //	System.setProperty("spark.io.compression.codec","org.apache.spark.io.LZFCompressionCodec")
    //        System.setProperty("spark.io.compression.codec","org.apache.spark.io.SnappyCompressionCodec")
    //       System.setProperty("spark.akka.frameSize", "10024")
    //	System.setProperty("spark.akka.timeout", "90000")
    //	System.setProperty("spark.worker.timeout", "90000")
    //	System.setProperty("spark.storage.blockManagerHeartBeatMs","30000")
    //        System.setProperty("spark.cores.max","8")
    //      System.setProperty("spark.local.dir", "/home/ubuntu")

  }
}
