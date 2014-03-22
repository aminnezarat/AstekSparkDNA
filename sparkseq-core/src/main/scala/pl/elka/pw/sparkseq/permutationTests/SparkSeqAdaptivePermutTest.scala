/**
 * Copyright (c) 2014. Marek Wiewiorka
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pl.elka.pw.sparkseq.permutationTests

import pl.elka.pw.sparkseq.statisticalTests._
import org.apache.commons.math3.util.ArithmeticUtils._

/**
 * Created by marek on 3/9/14.
 */

/**
 * Class for conducting adaptive permutation test.
 * @param iNPermut Number of permutations to run.
 * @param iStatTests Array of tests to run.
 * @param iX X Vector of coverage
 * @param iY Y Vector of coverage
 */
class SparkSeqAdaptivePermutTest(iNPermut: Int = 1000, iStatTests: Array[StatisticalTest], iX: Seq[Int], iY: Seq[Int]) {


  private var nTStatGE: Int = 0
  private val xVect = iX
  private val yVect = iY
  private val nX = xVect.length
  private val initVect = xVect ++ yVect
  private val n = initVect.length
  private val statTests = iStatTests
  private val nAllPermut = (factorial(n) / (factorial(n - nX) * factorial(nX))).toInt
  private val nPermut = math.min(iNPermut, nAllPermut)

  // private val permArray = new Array[(Array[Int],Array[Int])](nAllPermut)

  private def calculateTestStat(iX: Seq[Int], iY: Seq[Int]): Double = {
    val testResult = new Array[Double](statTests.length)
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
    var i = 1
    while (combIter.hasNext && i <= nPermut) {
      val listX = combIter.next()
      if (calculateTestStat(listX.toArray, (initList diff listX).toArray) >= initTStat)
        nTSGreatEqual += 1
      i += 1
    }
    nTSGreatEqual
  }

  /**
   * Method for getting exact p-value.
   * @return p-value.
   */
  def getPvalue(): Double = {
    nTStatGE = calculateNTStatGreatEqual()
    (nTStatGE.toDouble / nPermut.toDouble)
  }

}
