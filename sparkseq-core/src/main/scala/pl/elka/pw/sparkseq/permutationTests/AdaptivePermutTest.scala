package pl.elka.pw.sparkseq.permutationTests

import pl.elka.pw.sparkseq.statisticalTests._

/**
 * Created by mesos on 3/9/14.
 */
abstract class AdaptivePermutTest(iNPermut: Int = 1000, iStatTests: Array[StatisticalTest], iX: Seq[Int], iY: Seq[Int]) {


  private val nPermut = iNPermut
  private var nTStatGE: Int = 0
  private val xVect = iX
  private val yVect = iY
  private val initVect = xVect ++ yVect
  private val statTests = iStatTests

  def calculateTestStats(iX: Seq[Int], iY: Seq[Int]): Double = {
    //calculate initial T-stat
    var combTestStat = 0.0
    var testResult = Array[Double](statTests.length)
    var i = 0
    for (t <- statTests) {
      testResult(i) = t.getTestStatistics(iX, iY)
      i += 1
    }
    combineTestStats(testResult)
  }

  //def generatePermutArray()
  def combineTestStats(iTestStats: Array[Double]): Double

  def getPvalue(): Double = {
    nTStatGE / nPermut
  }

}
