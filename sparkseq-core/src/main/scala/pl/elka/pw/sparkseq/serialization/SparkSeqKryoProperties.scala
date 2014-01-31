package pl.elka.pw.sparkseq.serialization

/**
 * Object for setting various KryoSerializer properties.
 */
object SparkSeqKryoProperties {

  /**
   * Method used before creating Apache Spark context for setting various KryoSerializer properties.
   * @return
   */
  def setupKryoContextProperties() = {
    System.setProperty("spark.kryoserializer.buffer.mb", "20")
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "pl.elka.pw.sparkseq.serialization.SparkSeqKryoRegistrator")
    System.setProperty("spark.kryo.referenceTracking","false")
  }

}
