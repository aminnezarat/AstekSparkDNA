/**
 * Copyright (c) 2014. [insert your company or name here]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pl.elka.pw.sparkseq.util

/**
 * Object for setting various Apache Spark Context properties.
 */
object SparkSeqContexProperties {
  /**
   * Method used before creating Apache Spark context for setting various of its properties.
   * @return
   */
  def setupContexProperties() ={
    System.setProperty("spark.storage.memoryFraction", "0.5")
    System.setProperty("spark.executor.memory", "10g")
    //System.setProperty("spark.locality.wait","600")
    	System.setProperty("spark.rdd.compress", "true")
    //	System.setProperty("spark.io.compression.codec","org.apache.spark.io.LZFCompressionCodec")
    //        System.setProperty("spark.io.compression.codec","org.apache.spark.io.SnappyCompressionCodec")
    //       System.setProperty("spark.akka.frameSize", "10024")
    //	System.setProperty("spark.akka.timeout", "90000")
    //	System.setProperty("spark.worker.timeout", "90000")
    //	System.setProperty("spark.storage.blockManagerHeartBeatMs","30000")
    //System.setProperty(" spark.mesos.coarse","true")
    //System.setProperty("spark.cores.max","10")
    //      System.setProperty("spark.local.dir", "/home/ubuntu")

  }
}
