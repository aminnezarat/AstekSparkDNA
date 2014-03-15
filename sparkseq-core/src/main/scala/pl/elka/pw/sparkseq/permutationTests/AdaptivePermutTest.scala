package pl.elka.pw.sparkseq.permutationTests

import pl.elka.pw.sparkseq.statisticalTests._
import org.apache.commons.math3.util.ArithmeticUtils._

/**
 * Created by mesos on 3/9/14.
 */
class AdaptivePermutTest(iNPermut: Int = 1000, iStatTests: Array[StatisticalTest], iX: Seq[Int], iY: Seq[Int]) {


  private val nPermut = iNPermut
  private var nTStatGE: Int = 0
  private val xVect = iX
  private val yVect = iY
  private val nX = xVect.length
  private val initVect = xVect ++ yVect
  private val n = initVect.length
  private val statTests = iStatTests
  //private val nAllPermut = (factorial(n)/(factorial(n-nX)*factorial(nX)) ).toInt
  // private val permArray = new Array[(Array[Int],Array[Int])](nAllPermut)

  private def calculateTestStat(iX: Seq[Int], iY: Seq[Int]): Double = {
    val testResult = Array[Double](statTests.length)
    var i = 0
    for (t <- statTests) {
      testResult(i) = t.getTestStatistics(iX, iY)
      i += 1
    }
    combineTestStats(testResult)
  }

  /*  private def generatePermutArray() : Array[(Array[Int],Array[Int])] = {
      var i = 0
    }*/
  private def combineTestStats(iTestStats: Array[Double]): Double = {
    iTestStats.max
  }

  private def calculateNTStatGreatEqual(): Int = {
    val initTStat = calculateTestStat(xVect, yVect)
    val initList = initVect.toList
    var nTSGreatEqual = 0
    val combIter = initVect.toList.combinations(nX)
    while (combIter.hasNext) {
      val listX = combIter.next()
      if (calculateTestStat(listX.toArray, (initList diff listX).toArray) >= initTStat)
        nTSGreatEqual += 1
    }
    nTSGreatEqual
  }

  def getPvalue(): Double = {
    nTStatGE = calculateNTStatGreatEqual()
    nTStatGE / nPermut
  }

}
