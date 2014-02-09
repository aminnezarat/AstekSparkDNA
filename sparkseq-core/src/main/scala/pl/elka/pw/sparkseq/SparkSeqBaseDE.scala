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
    val posEnd=300000000
    val minAvgBaseCov = 10


    //./XCVMTest 7 7 | cut -f2,4 | sed 's/^\ \ //g' | grep "^[[:digit:]]" >cm7_7_2.txt
    val cmDistTable = sc.textFile("../sparkseq-core/src/main/resources/cm"+caseSampSize+"_"+controlSampSize+"_2.txt")
      .map(l => l.split("\t"))
      .map(r=>(r.array(0).toDouble,r.array(1).toDouble) )
      .toArray



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
      seqAnalysisCase.addBAM(sc,path,i,bamFileCountCaseFirst.toDouble/bamFileCount.toDouble)
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
      seqAnalysisControl.addBAM(sc,path,i,bamFileCountControlFirst.toDouble/bamFileCount.toDouble)

    }

    //compute coverage + filer out bases with mean cov < minAvgBaseCov + padding with 0 so that all vectors have the same length
    val covCase = seqAnalysisCase.getCoverageBaseRegion(chr,posStart,posEnd)
          .map(r=>(r._1%100000000000L,r._2)).groupByKey()
          //.filter(r=>(SparkSeqStats.mean(r._2) > minAvgBaseCov && r._2.length > caseSampSize/2) )
          .map(c=> if((caseSampSize-c._2.length)>0)(c._1,c._2++ArrayBuffer.fill[Int](caseSampSize-c._2.length)(0)) else (c._1,c._2) )
    val covControl = seqAnalysisControl.getCoverageBaseRegion(chr,posStart,posEnd)
        .map(r=>(r._1%100000000000L,r._2)).groupByKey()
        //.filter(r=>(SparkSeqStats.mean(r._2) > minAvgBaseCov && r._2.length > controlSampSize/2) )
    	  .map(c=> if((controlSampSize-c._2.length)>0)(c._1,c._2++ArrayBuffer.fill[Int](controlSampSize-c._2.length)(0)) else (c._1,c._2) )
    val leftCovJoint = covCase.leftOuterJoin(covControl).subtract(covCase.join(covControl.map(r=>(r._1,Option(r._2)) ) ) )
    val rightCovJoint = covCase.rightOuterJoin(covControl)

    //final join + compute Cramver von Mises test statistics + filtering
    val finalcovJoint = leftCovJoint.map(r=>(r._1,Option(r._2._1),r._2._2)).union(rightCovJoint.map(r=>(r._1,r._2._1,Option(r._2._2))) ).map(r=>(r._1,(r._2,r._3)))
        .map( r=> (r._1,(r._2._1 match {case Some(x) =>x;case None =>ArrayBuffer.fill[Int](caseSampSize)(0) },
                         r._2._2 match {case Some(x) =>x;case None =>ArrayBuffer.fill[Int](controlSampSize)(0) }) ) )
      .map(r=>(r._1,r._2,SparkSeqCvM2STest.computeTestStat(r._2._1,r._2._2) ) ).map(r=>((r._1),(r._2,r._3,SparkSeqCvM2STest.getPValue(r._3,cmDistTable)) ) )
      .filter(r=> ( SparkSeqStats.mean(r._2._1._1) > minAvgBaseCov || SparkSeqStats.mean(r._2._1._2) > minAvgBaseCov ) )
      .map(r=>(r._2._3,r._1)) //pick position and p-value
      .map(c=>if(c._1<0.001)(0.001,c._2) else if(c._1>=0.001 && c._1<0.01) (0.01,c._2) else if(c._1>=0.01 && c._1<0.05)(0.05,c._2) else (0.1,c._2) ) //make p-value discrete
      .groupByKey().sortByKey(true,8).map(r=>(r._1,r._2.sortBy(x=>x)) )
      .map{r=>
          var regLenArray:ArrayBuffer[(Int,Long)]=ArrayBuffer()
          var regStart = r._2(0)
          var regLength = 1
          var i = 1
          while(i<r._2.length){
          if(r._2(i)-1 != r._2(i-1) ){
            if(regLength>=minRegLength)
            regLenArray+=((regLength,regStart))
            regLength=1
          regStart=r._2(i)
          }
          else regLength+=1
            i=i+1
          }
          (r._1,regLenArray.sortBy(-_._1))
    }



    //.reduce((a,b) => if(a._1.max==b._1.min-1) (a._1++b._1,b._2) else if())

      //.filter(r=>r._1==1000029324L)
        //.count()
   // println(leftCovJoint)
//println(finalcovJoint)

  finalcovJoint.take(100).foreach(println)
    //println(rightCovJoint)

  }

}
