package pl.elka.pw.sparkseq.statisticalTests
import java.lang.Math._
import org.apache.commons.math3.distribution.ChiSquaredDistribution

class SparkSeqFisherCombProb extends Serializable{
	
	def computeStatistics(pvalArray:Array[Double] ) : (Double,Double) ={
	  var fisherStat = 0.0
	  for(v <- pvalArray){
	    fisherStat += -2*log(v)
	  }
	  //return tuple : fisher stat,degrees of freedom
	  return((fisherStat,2*pvalArray.length) )
	}
	
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