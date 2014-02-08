package pl.elka.pw.sparkseq
import pl.elka.pw.sparkseq.statisticalTests._
import org.apache.spark.SparkContext
import pl.elka.pw.sparkseq.seqAnalysis.SparkSeqAnalysis
import pl.elka.pw.sparkseq.statisticalTests._
import org.apache.spark.SparkContext._
import collection.mutable.ArrayBuffer
import org.apache.hadoop.io.LongWritable
import fi.tkk.ics.hadoop.bam.{BAMInputFormat, SAMRecordWritable}

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

    val caseIdFam1 = Array(38,39,42/*,44,45,47,53*/)
    val controlIdFam1 = Array(56,74,76/*,77,83,94*/)
    val caseIdFam2 = Array(100,111,29/*,36,52,55,64,69*/)
    val controlIdFam2 = Array(110,30,31/*,51,54,58,63,91,99*/)

    val caseSampSize = caseIdFam1.length + caseIdFam2.length + 1
    val controlSampSize = controlIdFam1.length + controlIdFam2.length  + 1

    val testSuff="_sort_chr1.bam"
    val chr = "chr1"
    val posStart=1
    val posEnd=1000000
    val minAvgBaseCov = 10


   //./XCVMTest 7 7 | cut -f2,4 | sed 's/^\ \ //g' | grep "^[[:digit:]]" >cm7_7_2.txt
    val cmDistTable = sc.textFile("../sparkseq-core/src/main/resources/cm7_7_2.txt").map(l => l.split("\t")).map(r=>(r.array(0).toDouble,r.array(1).toDouble) ).toArray



    /*val normArray=Array(1.0,8622606.0/19357579.0,8622606.0/14087644.0,8622606.0/18824924.0,8622606.0/9651030.0,8622606.0/22731556.0,
      8622606.0/15975604.0,8622606.0/17681528.0, 8622606.0/16323269.0,  8622606.0/18408612.0, 8622606.0/15934054.0, 8622606.0/22329258.0,
      8622606.0/14788631.0, 8622606.0/14346120.0, 8622606.0/ 16693869.0)
*/

    val seqAnalysisCase = new SparkSeqAnalysis(sc,pathFam1+"/Case/Sample_25"+testSuff,25,1,numTasks)
    val bamFileCountCaseFirst= sc.newAPIHadoopFile[LongWritable,SAMRecordWritable,BAMInputFormat](pathFam1+"/Case/Sample_25"+testSuff).count()
    for(i<-caseIdFam1++caseIdFam2){
      var path:String = ""
      if(caseIdFam1.contains(i))
        path = pathFam1+"/Case/Sample_"+i.toString+testSuff
      else
        path = pathFam2+"/Case/Sample_"+i.toString+testSuff
      val bamFileCount= sc.newAPIHadoopFile[LongWritable,SAMRecordWritable,BAMInputFormat](path).count()
      seqAnalysisCase.addBAM(sc,path,i,bamFileCountCaseFirst/bamFileCount)
    }



    val seqAnalysisControl = new SparkSeqAnalysis(sc,pathFam1+"/Control/Sample_26"+testSuff,26,1,numTasks)
    val bamFileCountControlFirst= sc.newAPIHadoopFile[LongWritable,SAMRecordWritable,BAMInputFormat](pathFam1+"/Control/Sample_26"+testSuff).count()
    for(i<-controlIdFam1++controlIdFam2){
      var path:String = ""
      if(controlIdFam1.contains(i))
        path = pathFam1+"/Control/Sample_"+i.toString+testSuff
      else
        path = pathFam2+"/Control/Sample_"+i.toString+testSuff
      val bamFileCount= sc.newAPIHadoopFile[LongWritable,SAMRecordWritable,BAMInputFormat](path).count()
      seqAnalysisControl.addBAM(sc,path,i,bamFileCountControlFirst/bamFileCount)

    }


    val covCase = seqAnalysisCase.getCoverageBaseRegion(chr,posStart,posEnd)
          .map(r=>(r._1%100000000000L,r._2)).groupByKey()
          .filter(r=>(SparkSeqStats.mean(r._2) > minAvgBaseCov && r._2.length > caseSampSize/2) )
          .map(c=> if((caseSampSize-c._2.length)>0)(c._1,c._2++ArrayBuffer.fill[Int](caseSampSize-c._2.length)(0)) else (c._1,c._2) )
    val covControl = seqAnalysisControl.getCoverageBaseRegion(chr,posStart,posEnd)
        .map(r=>(r._1%100000000000L,r._2)).groupByKey()
        .filter(r=>(SparkSeqStats.mean(r._2) > minAvgBaseCov && r._2.length > controlSampSize/2) )
    	  .map(c=> if((controlSampSize-c._2.length)>0)(c._1,c._2++ArrayBuffer.fill[Int](controlSampSize-c._2.length)(0)) else (c._1,c._2) )
    val leftCovJoint = covCase.leftOuterJoin(covControl).subtract(covCase.join(covControl.map(r=>(r._1,Option(r._2)) ) ) )
    val rightCovJoint = covCase.rightOuterJoin(covControl)
    val finalcovJoint = leftCovJoint.map(r=>(r._1,Option(r._2._1),r._2._2)).union(rightCovJoint.map(r=>(r._1,r._2._1,Option(r._2._2))) ).map(r=>(r._1,(r._2,r._3)))
        .map( r=> (r._1,(r._2._1 match {case Some(x) =>x;case None =>ArrayBuffer.fill[Int](caseSampSize)(0) },
                         r._2._2 match {case Some(x) =>x;case None =>ArrayBuffer.fill[Int](controlSampSize)(0) }) ) )
      .map(r=>(r._1,r._2,SparkSeqCvM2STest.computeTestStat(r._2._1,r._2._2) ) ).map(r=>((r._1),(r._2,r._3,SparkSeqCvM2STest.getPValue(r._3,cmDistTable)) ) )
      .fil
      .sortByKey(true,8)
        //.count()
   // println(leftCovJoint)
//println(finalcovJoint)

  finalcovJoint.take(100).foreach(println)
    //println(rightCovJoint)

  }

}
