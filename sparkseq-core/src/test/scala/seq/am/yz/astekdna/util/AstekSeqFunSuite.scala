package seq.am.yz.astekdna.util

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.SparkContext
import java.net.ServerSocket

import org.apache.log4j.Level
import seq.am.yz.astekdna.serialization.AstekSeqKryoProperties

import scala.util.Random

object AstekSeqTest extends org.scalatest.Tag("seq.am.yz.astekdna.util.AstekFunSuite")

trait AstekFunSuite extends FunSuite with BeforeAndAfter {

 val r = new scala.util.Random
 val randPort=(10000+math.abs(r.nextInt).toDouble/Int.MaxValue*50000).toInt
 System.setProperty("spark.ui.port",randPort.toString) 
 var sc: SparkContext = _


  def createSpark(sparkName: String): SparkContext = {
      new SparkContext("local[2]", sparkName)

  }

  def destroySpark() {
    // Stop the context
    sc.stop()
    sc = null

  }

  def sparkSeqTest(name: String)(body: => Unit) {
    test(name, AstekSeqTest) {
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
