package pl.elka.pw.sparkseq.util


import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.SparkContext
import java.net.ServerSocket
import org.apache.log4j.Level
import pl.elka.pw.sparkseq.serialization.SparkSeqKryoProperties
import scala.util.Random

object SparkSeqTest extends org.scalatest.Tag("pl.elka.pw.sparkseq.util.SparkFunSuite")

trait SparkFunSuite extends FunSuite with BeforeAndAfter {

 val r = new scala.util.Random
 val randPort=(10000+math.abs(r.nextInt).toDouble/Int.MaxValue*50000).toInt
 System.setProperty("spark.ui.port",randPort.toString) 
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
