package pl.elka.pw.sparkseq.statisticalTests
import scala.util.control._

/**
 * Object for computing two-sample Cramer-von Mises test
 */
object SparkSeqCvM2STest extends Serializable{
  /**
   * Method for computing test statistics of two-sample Cramer von Mises test
   * @param x Value from the first sample.
   * @param y Valure from the other sample.
   * @return Test statistics.
   */
	def computeTestStat(x:Seq[Int],y:Seq[Int]) : Double ={
	  
	  var T2 = 0.0
	  val n = x.length
	  val m = y.length
	  val mn = n+m
	  val xSort = x.sortBy(x=>x)
	  val ySort = y.sortBy(y=>y)
	  val xyComb = x++y
	  val xySort = xyComb.sortBy(x=>x)
	  var xRank = Array.fill[Double](n)(0.0)
	  var yRank = Array.fill[Double](m)(0.0)
	  var searchVal = 0.0
	  var k = 0
	  while(k < mn){
		searchVal = xySort(k)  
	    if( k == mn-1 || (xySort(k) != xySort(k+1)) ){
	      val loop = new Breaks;
       		loop.breakable {
       			for(i<- 0 to n-1){
       				if(xSort(i) == searchVal){
       					xRank(i) = k+1
       					loop.break
       				}
       				
       			}
       			for(j <- 0 to m-1){
       			  if(ySort(j) == searchVal){
       					yRank(j) = k+1
       					loop.break
       				}
       			}
       		}
       		k+=1
	    }
	    else if( (k < mn-1) && (xySort(k) == xySort(k+1) ) ){
	      //find no of duplicates
       		var p = k+1
       		var dupCount = 2
       		val loop = new Breaks;
       			loop.breakable {
       				while( p< mn-1){
       					if(xySort(p) == xySort(p+1))
       					  dupCount+=1
       					 else
       					   loop.break
       					p+=1
       				}	
       			}
       		//set the average value to (k+p)/2	in both xRanks and yRanks
       		var avgVal = (k.toDouble+p.toDouble+2.0)/2
	      	for(i<- 0 to n-1){
	      		if(xSort(i) == searchVal)
       				xRank(i) = avgVal
       			
       		}
       		for(j <- 0 to m-1){
       			if(ySort(j) == searchVal)
       				yRank(j) = 	avgVal
       		}
	      k=p+1
	    }
	  }
	  /*compute T2 statistics, T2 = U/(N*M*(M+N)) - (4*M*N-1)/(6*(M+N))*/
	  //
/*	  xRank.foreach(x =>println(x))
	  println("****")
	  yRank.foreach(x =>println(x))*/
	  var U = 0.0
	  var sumXRank = 0.0
	  var sumYRank = 0.0
	  for(i <- 0 to n-1){
	    sumXRank +=(xRank(i)-(i+1))*(xRank(i)-(i+1))
	  }
	  for(j <- 0 to m-1){
	    sumYRank +=(yRank(j)-(j+1))*(yRank(j)-(j+1))
	  }
	  U = n*sumXRank + m*sumYRank
	  T2 = U/(n.toDouble*m.toDouble*(m.toDouble+n.toDouble)) - (4*n.toDouble*m.toDouble-1)/(6*(m.toDouble+n.toDouble))
	  return(T2)
	}

  /**
   * Method for computing p-value for a given Cramer von Mises test statistics and distance Table
   * @param t2 Test statistics
   * @param distTable Distance table
   * @return p-value
   */
	def getPValue(t2:Double,distTable:org.apache.spark.broadcast.Broadcast[Array[(Double,Double)]]):Double ={
	  
	  var pvalue:Double = 1.0
	  val loop = new Breaks;
	 // println(t2)
	  if(t2 == 0.0) return(1.0)
      loop.breakable {
		  for(i <- 0 to distTable.value.length-1){
		    if(t2<=distTable.value(i)._1 && i >1 ){
		      pvalue = 1-distTable.value(i-1)._2
		       loop.break
		    }
		  /* if(t2<=distTable(i)._1 && distTable(i)._2<0.5 && i >1){
		        pvalue = distTable(i-1)._2
		        loop.break
		    }
		    else if (t2<=distTable(i)._1 &&	distTable(i)._2>=0.5 && i>=1){
		      pvalue = 1-distTable(i-1)._2
		      loop.break
		    }*/
		  }
      }
	  return(pvalue)
	}
}
