package pl.elka.pw.sparkseq.seqAnalysis

import org.apache.spark.SparkContext
import SparkContext._
import org.apache.spark.rdd._
import org.apache.spark._
import fi.tkk.ics.hadoop.bam.BAMInputFormat
import fi.tkk.ics.hadoop.bam.SAMRecordWritable
import org.apache.hadoop.io.LongWritable
import scala.util.control._
import scala.collection.mutable.ArrayBuffer
import pl.elka.pw.sparkseq.conversions.SparkSeqConversions

/**
 * Main class for analysis of sequencing data. A SparkSeqAnalysis holds Apache Spark context as well as references
 * to BAM files containing NGS data.
 *
 * @param iSC Apache Spark context.
 * @param iBAMFile  Path to the first BAM file.
 * @param iSampleId  Id of the firs sample (must be numeric).
 * @param iNormFactor  Normalization factor for doing count normalization between samples.
 * @param iReduceWorkers  Number of Reduce workers for doing transformations such as sort or join (see
 *                       http://spark.incubator.apache.org/docs/latest/scala-programming-guide.html for details).
 */
 class SparkSeqAnalysis(iSC: SparkContext, iBAMFile:String,iSampleId:Int,iNormFactor:Double, iReduceWorkers:Int=8) extends Serializable
{
	
  /* Spark context parameters defaults */	
/*  val workerMem = iWorkerMem getOrElse "6g"
  val serializer = iSerializer getOrElse "spark.KryoSerializer" 
  val masterConnString = iMasterConnString getOrElse "local"
  val sparkHome = iSparkHome getOrElse "/opt/spark"*/
  
    //val sc = iSC
  /**
   * References to all samples in the analysis.
   */
  	var bamFile= iSC.newAPIHadoopFile[LongWritable,SAMRecordWritable,BAMInputFormat](iBAMFile).map(r=>(iSampleId,r))
  	private var bamFileFilter = bamFile
    //val bedFile= iSC.textFile(iBEDFile)
      
   // val genExons = iSC.textFile(iBEDFile)
//		   			.map(l => l.split("\t"))
//		   			.map(r=>(r.array(4).trim,r.array(5).toInt,r.array(0),r.array(1).toInt,r.array(2).toInt,r.array(3)) ).toArray
		   											/*genExons format: (genId,ExonId,chr,start,end,strand)*/
   /* val genExonsMap = scala.collection.mutable.HashMap[(String,Int,Int),(String,Int)]()
    for(ge <- genExons)
      genExonsMap((ge._3,ge._4,ge._5)) = ((ge._1,ge._2))*/
    //print(genExons.count)
    //iSC.broadcast(genExons)
		   			
    private var normFactor = scala.collection.mutable.HashMap[Int,Double]()
    normFactor(iSampleId) = iNormFactor
	//var bedFile:RDD[String] = null
	//val fastaFile = iFASTAFile
  /**
   *  Method for generating bases coordinates that a given read is alligned to using its Cigar string.
   *
   * @param iAlignStart Start of a read alignment
   * @param iCigar Cigar string of a read aligments
   * @return Array of ranges computed from Cigar string.
   */
  private def genBasesFromCigar(iAlignStart:Int, iCigar:net.sf.samtools.Cigar ): Array[Range] = {
    
    var nuclReadArray = ArrayBuffer[Range]()
    val numCigElem = iCigar.numCigarElements()
    
   
    var nuclShift = 0
    for(i <- 0 to (numCigElem-1)){
      var cElem = iCigar.getCigarElement(i)	
      //first mapped read fragment
      if(cElem.getOperator().toString()=="M" && i ==0 ||(i==1 && iCigar.getCigarElement(0).getOperator().toString()=="S" ) )
    	  //nuclReadArray=Array.range(iAlignStart,iAlignStart+cElem.getLength()+1)
        nuclReadArray+=Range(iAlignStart,iAlignStart+cElem.getLength()+1)
      //find maps in between	  
      else if(cElem.getOperator().toString()!="M" )
    	  nuclShift+=cElem.getLength()
      else if(cElem.getOperator().toString()=="M" && i>1 && i<(numCigElem-1 ) && nuclReadArray.length>0){ 	 
          var mapStr=nuclReadArray.last.last+nuclShift
    	  //nuclReadArray=Array.concat(nuclReadArray,Array.range(mapStr,mapStr+cElem.getLength()+1))
          nuclReadArray+=Range(mapStr,mapStr+cElem.getLength()+1)
    	  nuclShift=0  		
      }
       //last mapped read fragment
       else if(cElem.getOperator().toString()=="M" && i == (numCigElem-1) && nuclReadArray.length>0)
         //nuclReadArray=Array.concat(nuclReadArray,Array.range(nuclReadArray.last+nuclShift,nuclReadArray.last+nuclShift+cElem.getLength()+1))    
         nuclReadArray+=Range(nuclReadArray.last.last+nuclShift,nuclReadArray.last.last+nuclShift+cElem.getLength()+1)
    }     
    return nuclReadArray.toArray
    
  }


  /**
   * Method for adding another BAM files to intance of SparkSeqAnalysis class.
   *
   * @param iSC Apache Spark context.
   * @param iBAMFile  Path to the first BAM file.
   * @param iSampleId  Id of the firs sample (must be numeric).
   * @param iNormFactor  Normalization factor for doing count normalization between samples.
   *
   */
  def addBAM(iSC: SparkContext,iBAMFile:String,iSampleId:Int,iNormFactor:Double){
    bamFile=bamFile++iSC.newAPIHadoopFile[LongWritable,SAMRecordWritable,BAMInputFormat](iBAMFile).map(r=>(iSampleId,r))
    normFactor(iSampleId) = iNormFactor
    bamFileFilter = bamFile
  }

  /**
   *Method for computing coverage for a given list of genetic regions.
   *
   * @param iGenExons A Spark broadcast variable created from BED file that is transformed using SparkSeqConversions.BEDFileToHashMap
   * @return RDD of tuples (regionId, coverage)
   */
def getCoverageRegion(iGenExons:org.apache.spark.broadcast.Broadcast[scala.collection.mutable.HashMap[String,Array[scala.collection.mutable.ArrayBuffer[(Int, Int, Int, Int)]]]]):RDD[(Long,Int)] ={

//iGenExons:scala.collection.mutable.HashMap[ String,Array[ArrayBuffer[(Int,Int,Int,Int)/*(GeneId,ExonId,Start,End)*/] ] ]):RDD[(Long,Int)] = {

         //val genExonsB=iGenExons.value 
	 val coverage = (bamFileFilter.mapPartitions{
	    partitionIterator =>
	 	//var exonsCountArray = new Array[(Long,Int)](3000000)
		var exonsCountMap = scala.collection.mutable.HashMap[Long,Int]()
	 	var sampleId:Long = 0
	 	var exId=0
	 	var refName:String =""
	 	for (read <- partitionIterator){
	 	   sampleId = read._1*1000000000000L
	 	   refName = read._2._2.get.getReferenceName
	 	   if( iGenExons.value.contains(refName) ){
			var exons = iGenExons.value(refName)
			var basesFromRead = genBasesFromCigar(read._2._2.get.getAlignmentStart, read._2._2.get.getCigar)
			for(basesArray<-basesFromRead){
			    var subReadStart = basesArray.start
			    var subReadEnd = basesArray.end
			    var idReadStart = subReadStart/10000
			    var idReadEnd = subReadEnd/10000
			    var readStartArray = exons(idReadStart)
			    if(idReadStart > 0 && readStartArray != null && exons(idReadStart-1) != null)
			 	readStartArray=readStartArray++(exons(idReadStart-1))
			    else if(idReadStart > 0 && readStartArray == null && exons(idReadStart-1) != null)  
			  	readStartArray = exons(idReadStart-1)
			    val loop = new Breaks;
			 		    	
			   // if(idReadStart == idReadEnd ){
				 if(readStartArray != null){
				     for(es <- readStartArray)	{  
				 	loop.breakable{
				 	   for(r <-subReadStart to subReadEnd by 2) {
				 		if(es._3<=r && es._4>=r){
                   				    var id = sampleId+es._1*100000L+es._2
                                                   if(!exonsCountMap.contains(id) )
							exonsCountMap(id) = 1
						    else
						       exonsCountMap(id) += 1	  
				 		      //exonsCountArray(exId)= (id,1)
				 		    //exId+=1
				 		    loop.break
				 		}
				 		    		      
				 	    }
					}
				      }
				 }
			   // }
				 		   
			   }
			 		    	
		     }
		}
	//}
        /*for(e <- exonsCountMap){
	  exonsCountArray(exId) = (e._1,e._2) 
	  exId+=1	
	}*/
	
     	Iterator(exonsCountMap)	
	 }
	 ).flatMap(r=>r).reduceByKey(_+_, iReduceWorkers)
   return(coverage)
} 
  
 /* returns (sampleId,GeneName,ExonId,Count)*/
/* def getCoverageExonOLD():RDD[((Int,String,Int),Int)] = {
 
      
     val coverage = (bamFileFilter.mapPartitions{partitionIterator =>
     
     	var sampleId = 0
     	var id:Int = 0
     	var i = 0 
     	var count = 0
     	var minIndex = Int.MaxValue
     	var maxIndex = 0
		val bufferSize =1000
		var refName:String =""
		var chNumCode:Long = 0
		var exonsCountMap = scala.collection.mutable.HashMap[(String,Int),Int]()
		var exonsCountArray = new Array[((Int,String,Int),Int)](500000)  
     	for (read <- partitionIterator){
     		sampleId = read._1
     		refName = read._2._2.get.getReferenceName
     		var exons = genExons.filter(r=>(r._3==refName))
            //chNumCode = chrToLong(refName)+sampleId*1000000000000L
            var basesFromRead = genBasesFromCigar(read._2._2.get.getAlignmentStart, read._2._2.get.getCigar)
            for(basesArray<-basesFromRead){
              var subReadStart = basesArray.start
              var subReadEnd = basesArray.end
             
              exons = exons.filter(e=> (e._4 >= subReadStart && e._4<= subReadEnd &&  e._5 >= subReadEnd) || 
                     ( e._4 <= subReadStart  && e._5 >= subReadStart &&  e._5 <= subReadEnd)  || (e._4 >= subReadStart  &&  e._5 <= subReadEnd) ) 
              if(exons.length>0){
	              for(ex <- exons){
	                if(exonsCountMap.contains( (ex._1,ex._2)) )
	                  exonsCountMap((ex._1,ex._2)) += 1
	                else
	                  exonsCountMap((ex._1,ex._2)) = 1
	              }
              }
            }
      	  
	 }
	 for(exCount <- exonsCountMap){
	   exonsCountArray(i) = ((sampleId,exCount._1._1,exCount._1._2),exCount._2)
	   i+=1
	 }
   Iterator(exonsCountArray.filter(r=>r!=null))
   }).flatMap(r=>r).reduceByKey(_+_, iReduceWorkers)
   return(coverage)
 }
  
def getCoverageExonOLD2():RDD[((Int,String,Int),Int)] = {
 
      
     val coverage = (bamFileFilter.mapPartitions{partitionIterator =>
     
     	var sampleId = 0
     	var id:Int = 0
     	var i = 0 
     	var count = 0
     	var minIndex = Int.MaxValue
     	var maxIndex = 0
	val bufferSize =1000
	var refName:String =""
	var chNumCode:Long = 0
	var chrMin = scala.collection.mutable.HashMap[String,Int]()
	var chrMax = scala.collection.mutable.HashMap[String,Int]()
	var exonsCountMap = scala.collection.mutable.HashMap[(String,Int),Int]()
		
	var chrMap = scala.collection.mutable.HashMap[Long,Array[(Int,Int)] ]()
		
	var exonsCountArray = new Array[((Int,String,Int),Int)](50000)
	var readsArray = new Array[(Long,Int,Int)](2000000)
     	for (read <- partitionIterator){
     		sampleId = read._1
     		refName = read._2._2.get.getReferenceName
     		chNumCode = chrToLong(refName)
     		if(!chrMin.contains(refName))
      	      chrMin(refName)=Int.MaxValue
      	    if(chrMin(refName)>read._2._2.get.getAlignmentStart)
      	      chrMin(refName) = read._2._2.get.getAlignmentStart  
      	    
      	    if(!chrMax.contains(refName))
      	      chrMax(refName)= 0 
      	    if(chrMax(refName)< read._2._2.get.getAlignmentEnd)
      	      chrMax(refName) = read._2._2.get.getAlignmentEnd
      	    var basesFromRead = genBasesFromCigar(read._2._2.get.getAlignmentStart, read._2._2.get.getCigar)
      	    for(bRange <- basesFromRead){
      	    	readsArray(i) = (chNumCode,bRange.start,bRange.end)
      	    	i+=1
      	    }
	 }
     readsArray = readsArray.filter(r=>r!=null)
     //filter exons according to min/max coordinates from the 1st pass over partition	
     var genExonsPruned= Array[(java.lang.String, Int, java.lang.String, Int, Int, java.lang.String)]()
     for(chr <- chrMax){
       
       genExonsPruned=Array.concat(genExonsPruned,genExons.filter(e=>(e._3 == chr._1 ) && (e._4 >= chrMin(chr._1) && e._4<= chrMax(chr._1) &&  e._5 >= chrMax(chr._1) || 
                    ( e._4 <= chrMin(chr._1)  && e._5 >= chrMin(chr._1) &&  e._5 <= chrMax(chr._1))  || (e._4 >= chrMin(chr._1)  &&  e._5 <= chrMax(chr._1) ) ) )   )
     }
     genExonsPruned = genExonsPruned.filter(e=>e!=null)
     i = 0
     for(ex <- genExonsPruned){
       if(ex != null){
	       var chr = chrToLong(ex._3)
	       var subReadStart = ex._4
	       var subReadEnd = ex._5
	       //var count = readsArray.filter(r=>(r._1 == chr && (r._2 >= subReadStart && r._2<= subReadEnd &&  r._3 >= subReadEnd) || 
	       //              ( r._2 <= subReadStart  && r._3 >= subReadStart &&  r._3 <= subReadEnd)  || (r._2 >= subReadStart  &&  r._3 <= subReadEnd))   ).length
	       var count = 1
	       if(count>0)
	           //exonsCountMap((ex._1,ex._2)) = count
	           exonsCountArray(i) = ((sampleId,ex._1,ex._2),count)
	           i+=1
	       }
     }
     /*i = 0
	 for(exCount <- exonsCountMap){
	   exonsCountArray(i) = ((sampleId,exCount._1._1,exCount._1._2),exCount._2)
	   i+=1
	 }*/
   Iterator(exonsCountArray.filter(r=>r!=null))
   }).flatMap(r=>r).reduceByKey(_+_, iReduceWorkers)
   return(coverage)
 }
*/
  /**
   * Method for computing coverage of all bases.
   * @return RDD of tuples (genID, coverage)
   */
  def getCoverageBase() : RDD[(Long,Int)]= {
		val coverage = (bamFileFilter.mapPartitions{partitionIterator =>
		var sampleId = 0
		var id:Int = 0
		var i = 0 
		var count = 0
		var minIndex = Int.MaxValue
		var maxIndex = 0
		var chrMap = scala.collection.mutable.HashMap[Long,Array[Array[Int]]]()
		var chrMin = scala.collection.mutable.HashMap[Long,Int]()
		var chrMax = scala.collection.mutable.HashMap[Long,Int]()
		val bufferSize =150000
		var refName:String =""
		var chNumCode:Long = 0
		//val nuclArray = new Array[Array[Int]](2000000)
		var countArray = new Array[(Long,Int)](8000000)
		var countArrayToReduce = new Array[(Long,Int)](200000)
		var outputArray = new   Array[Array[(Long,Int)]](2)

		for (read <- partitionIterator){
			sampleId = read._1
		    refName = read._2._2.get.getReferenceName
      	    chNumCode = SparkSeqConversions.chrToLong(refName)+sampleId*1000000000000L
      	    
      	    if(!chrMin.contains(chNumCode))
      	      chrMin(chNumCode)=Int.MaxValue
      	    if(chrMin(chNumCode)>read._2._2.get.getAlignmentStart)
      	      chrMin(chNumCode) = read._2._2.get.getAlignmentStart  
      	    
      	    if(!chrMax.contains(chNumCode))
      	      chrMax(chNumCode)= 0 
      	    if(chrMax(chNumCode)< read._2._2.get.getAlignmentEnd)
      	      chrMax(chNumCode) = read._2._2.get.getAlignmentEnd    
      		var basesFromRead = genBasesFromCigar(read._2._2.get.getAlignmentStart, read._2._2.get.getCigar)
      		//new chr in reads
      		if(!chrMap.contains(chNumCode))
      			   chrMap(chNumCode) = new Array[Array[Int]](2500000)
      		for(basesArray<-basesFromRead){
       		  for(rb<-basesArray)
      		  {
      		    //id = chNumCode+baseRange(j)
      			 id = (rb % 100)
      			 var idIn=rb/100
     			 if( chrMap(chNumCode)(idIn) == null)
      				 chrMap(chNumCode)(idIn) = Array.fill(100)(0)
      			 chrMap(chNumCode)(idIn)(id)+=1
      		  }
      		}
      		
      	}
		i=0
		var k = 0
		for(chr<-chrMap){  
		  for(j<-0 to chr._2.length-1){
			if(chr._2(j) != null){
				for(r<-0 to 99){
					if(chr._2(j)(r)>0){
						var idx = j*100+r
						if( idx<=(chrMin(chr._1)+bufferSize) || idx>=(chrMax(chr._1)-bufferSize) ){
							countArrayToReduce(k) = ( (chr._1+idx, math.round(chr._2(j)(r) *normFactor(sampleId)).toInt) )
							k+=1		
						}
						else{
							countArray(i) = ( (chr._1+idx, math.round(chr._2(j)(r) *normFactor(sampleId)).toInt ) )
						    i+=1
						}
			    		
					}	
				}  
			}
		  
		  }
		}
		outputArray(0) = countArray.filter(r=>r!=null)
		outputArray(1) = countArrayToReduce.filter(r=>r!=null)
		Iterator(outputArray)
    } )
    val coverageToReduce = coverage.flatMap(r =>(r.array(1)) ).reduceByKey(_+_, iReduceWorkers)
    val coverageNotReduce = coverage.flatMap(r =>(r.array(0)) )
    bamFileFilter=bamFile
    return (coverageNotReduce.union(coverageToReduce) )
  }

  /**
   * Method for computing coverage of all bases from a give chromosome region.
   *
   * @param chr Chromosome (eg. chr1)
   * @param regStart Starting position in a chromosome.
   * @param regEnd End position in a chromosome.
   * @return RDD of tuples (genID, coverage)
   */
  def getCoverageBaseRegion(chr:String, regStart:Int, regEnd:Int): RDD[(Long,Int)]={
    //val chrCode = chrToLong(chr)
    if(chr=="*")
    	bamFileFilter=bamFile.filter(r => r._2._2.get.getAlignmentStart()>=regStart && r._2._2.get.getAlignmentEnd()<=regEnd)
    else  
    	bamFileFilter=bamFile.filter(r => r._2._2.get.getReferenceName()==chr && r._2._2.get.getAlignmentStart()>=regStart && r._2._2.get.getAlignmentEnd()<=regEnd)
    
    return(getCoverageBase())
    
  }
  

    
  
}
  
