package pl.elka.pw.sparkseq.util


import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.SparkContext
import java.net.ServerSocket
import org.apache.log4j.Level
import pl.elka.pw.sparkseq.serialization.SparkSeqKryoProperties

object SparkSeqTest extends org.scalatest.Tag("pl.elka.pw.sparkseq.util.SparkFunSuite")

trait SparkFunSuite extends FunSuite with BeforeAndAfter {

  var sc: SparkContext = _


  def createSpark(sparkName: String): SparkContext = {
      new SparkContext("local[4]", sparkName)

  }

  def destroySpark() {
    // Stop the context
    sc.stop()
    sc = null

  }

  def sparkSeqTest(name: String)(body: => Unit) {
    test(name, SparkSeqTest) {
      sc = createSpark(name)
      try {
        // Run the test
        body
      }
      finally {
        destroySpark()
      }
    }
  }

}