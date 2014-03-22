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

import java.lang.Math._
import org.apache.commons.math3.distribution.ChiSquaredDistribution

/**
 * Object for computing combined p-values using Fisher's method
 */
object SparkSeqFisherCombProb extends Serializable {
  /**
   * Method for combining p-values from various tests.
   * @param pvalArray Array of p-values from other statistical tests.
   * @return Fisher's combined test statistics.
   */
  def computeStatistics(pvalArray: Array[Double]): (Double, Double) = {
    var fisherStat = 0.0
    for (v <- pvalArray) {
      fisherStat += -2 * log(v)
    }
    //return tuple : fisher stat,degrees of freedom
    return ((fisherStat, 2 * pvalArray.length))
  }

  /**
   * Method for computing a combined p-value using Fisher's method.
   * @param fStat Fisher's combined test statistics.
   * @param dof Degrees of freedom basing on a number of combined p-values.
   * @return Combined p-value.
   */
  def getPValue(fStat: Double, dof: Double): Double = {

    var pvalue = 1.0
    val chiSquareDist = new ChiSquaredDistribution(dof)
    pvalue = chiSquareDist.cumulativeProbability(fStat)
    if (pvalue > 0.5)
      pvalue = 1 - pvalue
    return (pvalue)
    1
  }

}