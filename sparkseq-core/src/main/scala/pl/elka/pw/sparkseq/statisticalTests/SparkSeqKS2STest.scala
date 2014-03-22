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

package pl.elka.pw.sparkseq.statisticalTests

import jsc.independentsamples.SmirnovTest

/**
 * Created by mesos on 3/17/14.
 */

/**
 * Object for computing two-sample Cramer-von Mises test
 */
object SparkSeqKS2STest extends SparkSeqStatisticalTest {

  /**
   * Method for computing test statistics of two-sample Cramer von Mises test
   * @param x Value from the first sample.
   * @param y Value from the other sample.
   * @return Test statistics.
   */
  def getTestStatistics(x: Seq[Int], y: Seq[Int]): Double = {

    val dX = x.map(r => r.toDouble).toArray
    val dY = y.map(r => r.toDouble).toArray
    val testKS = new SmirnovTest(dX, dY)
    return testKS.getTestStatistic
  }

}
