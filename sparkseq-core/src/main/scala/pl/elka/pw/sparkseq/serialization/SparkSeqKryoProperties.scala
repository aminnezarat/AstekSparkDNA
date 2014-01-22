package pl.elka.pw.sparkseq.serialization

/**
 * Created by marek on 1/22/14.
 */
object SparkSeqKryoProperties {

  def setupKryoContextProperties() = {
    System.setProperty("spark.kryoserializer.buffer.mb", "20")
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "pl.elka.pw.sparkseq.serialization.SparkSeqKryoRegistrator")
  }

}
