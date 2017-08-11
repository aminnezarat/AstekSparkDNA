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
package seq.am.yz.astekdna.statisticalTests

import jsc.independentsamples.MannWhitneyTest

/**
 * Created by Nezarat on 8/11/2017.
 */

/**
 * Object for computing two-sample Mann-Whitney  test
 */
object AstekSeqMW2STest extends AstekSeqStatisticalTest {

  /**
   * Method for computing test statistics of two-sample Mann-Whitney test
   * @param x Value from the first sample.
   * @param y Value from the other sample.
   * @return Test statistics.
   */
  def getTestStatistics(x: Seq[Int], y: Seq[Int]): Double = {

    val dX = x.map(r => r.toDouble).toArray
    val dY = y.map(r => r.toDouble).toArray
    val testMW = new MannWhitneyTest(dX, dY)

    return testMW.getTestStatistic

  }
}
