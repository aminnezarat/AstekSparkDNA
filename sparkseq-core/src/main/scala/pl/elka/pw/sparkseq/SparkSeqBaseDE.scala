package pl.elka.pw.sparkseq
import pl.elka.pw.sparkseq.statisticalTests._
import org.apache.spark.SparkContext
import pl.elka.pw.sparkseq.seqAnalysis.SparkSeqAnalysis
import org.apache.spark.SparkContext._
import collection.mutable.ArrayBuffer

/**
 * Created by marek on 2/8/14.
 */
object SparkSeqBaseDE {

  def main(args: Array[String]) {
    val sc = new  SparkContext("local[8]", "sparkseq", "/opt/spark-0.9.0-incubating")
    val fileSplitSize = 64
    val rootPath="/mnt/software/Phd_datastore/RAO/"
    val pathFam1 = rootPath+fileSplitSize.toString+"MB/condition_9/Fam1"
    val pathFam2 = rootPath+fileSplitSize.toString+"MB/condition_9/Fam2"
    val numTasks = 30
    val minCount = 10
    val minRegLength= 10
    val minCoverageRatio = 0.33
    val pval = 0.1

    val caseIdFam1 = Array(38,39/*,42,44,45,47,53*/)
    val controlIdFam1 = Array(56,74/*,76,77,83,94*/)
    val caseIdFam2 = Array(100,111/*,29,36,52,55,64,69*/)
    val controlIdFam2 = Array(110,30/*,31,51,54,58,63,91,99*/)

    val caseSampSize = caseIdFam1.length + caseIdFam2.length + 1
    val controlSampSize = controlIdFam1.length + controlIdFam2.length  + 1

    val testSuff="_sort_chr1.bam"
    val chr = "chr1"
    val posStart=1
    val posEnd=1000000
    val minAvgBaseCov = 1




    val normArray=Array(1.0,8622606.0/19357579.0,8622606.0/14087644.0,8622606.0/18824924.0,8622606.0/9651030.0,8622606.0/22731556.0,
      8622606.0/15975604.0,8622606.0/17681528.0, 8622606.0/16323269.0,  8622606.0/18408612.0, 8622606.0/15934054.0, 8622606.0/22329258.0,
      8622606.0/14788631.0, 8622606.0/14346120.0, 8622606.0/ 16693869.0)


    val seqAnalysisCase = new SparkSeqAnalysis(sc,pathFam1+"/Case/Sample_25"+testSuff,25,1,numTasks)
    var id = 1
    for(i<-caseIdFam1){
      seqAnalysisCase.addBAM(sc,pathFam1+"/Case/Sample_"+i.toString+testSuff,i,1); id+=1}

    for(i<-caseIdFam2){
      seqAnalysisCase.addBAM(sc,pathFam2+"/Case/Sample_"+i.toString+testSuff,i,1); id+=1}



    val seqAnalysisControl = new SparkSeqAnalysis(sc,pathFam1+"/Control/Sample_26"+testSuff,26,1,numTasks)
    for(i<-controlIdFam1){
      seqAnalysisControl.addBAM(sc,pathFam1+"/Control/Sample_"+i.toString+testSuff,i,1 ); id+=1}

    for(i<-controlIdFam2){
      seqAnalysisControl.addBAM(sc,pathFam2+"/Control/Sample_"+i.toString+testSuff,i,1 ); id+=1}


    val covCase = seqAnalysisCase.getCoverageBaseRegion(chr,posStart,posEnd).map(r=>(r._1%100000000000L,r._2)).groupByKey()
        .filter(r=>(SparkSeqStats.mean(r._2) > minAvgBaseCov) )
    	  .map(c=> if(caseSampSize-c._2.length>0)(c._1,c._2++ArrayBuffer.fill[Int](caseSampSize-c._2.length)(0)) else (c._1,c._2) )
    	//.map(c=> if(SparkSeqStats.mean(c._2) < 1 )(c._1,ArrayBuffer.fill[Int](caseSampSize)(0)) else(c._1,c._2)  )
    val covControl = seqAnalysisControl.getCoverageBaseRegion(chr,posStart,posEnd).map(r=>(r._1%100000000000L,r._2)).groupByKey()
        .filter(r=>(SparkSeqStats.mean(r._2) > minAvgBaseCov) )
    	  .map(c=> if(caseSampSize-c._2.length>0)(c._1,c._2++ArrayBuffer.fill[Int](caseSampSize-c._2.length)(0)) else (c._1,c._2) )
     // .map(c=> if(SparkSeqStats.mean(c._2) < 1 )(c._1,ArrayBuffer.fill[Int](caseSampSize)(0)) else(c._1,c._2)  )
    val leftCovJoint = covCase.leftOuterJoin(covControl).subtract(covCase.join(covControl.map(r=>(r._1,Option(r._2)) ) ) )
    val rightCovJoint = covCase.rightOuterJoin(covControl)
    val finalcovJoint = leftCovJoint.map(r=>(r._1,Option(r._2._1),r._2._2)).union(rightCovJoint.map(r=>(r._1,r._2._1,Option(r._2._2))) ).map(r=>(r._1,(r._2,r._3))).sortByKey(true,8)
   // println(leftCovJoint)
//println(finalcovJoint)

  finalcovJoint.take(100).foreach(println)
    //println(rightCovJoint)

  }

}
