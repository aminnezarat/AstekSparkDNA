package pl.elka.pw.sparkseq.statisticalTests
import java.lang.Math._
import org.apache.commons.math3.distribution.ChiSquaredDistribution

/**
 * Object for computing combined p-values using Fisher's method
 */
object SparkSeqFisherCombProb extends Serializable{
  /**
   * Method for combining p-values from various tests.
   * @param pvalArray Array of p-values from other statistical tests.
   * @return Fisher's combined test statistics.
   */
	def computeStatistics(pvalArray:Array[Double] ) : (Double,Double) ={
	  var fisherStat = 0.0
	  for(v <- pvalArray){
	    fisherStat += -2*log(v)
	  }
	  //return tuple : fisher stat,degrees of freedom
	  return((fisherStat,2*pvalArray.length) )
	}

  /**
   * Method for computing a combined p-value using Fisher's method.
   * @param fStat Fisher's combined test statistics.
   * @param dof Degrees of freedom basing on a number of combined p-values.
   * @return Combined p-value.
   */
	def getPValue(fStat:Double,dof:Double) : Double ={
	  
	  var pvalue = 1.0
	  val chiSquareDist = new ChiSquaredDistribution(dof)
	  pvalue = chiSquareDist.cumulativeProbability(fStat)
	  if (pvalue > 0.5)
	    pvalue=1-pvalue
	  return(pvalue)
1	  
	}
  
}