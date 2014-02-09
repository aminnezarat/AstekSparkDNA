package pl.elka.pw.sparkseq
import pl.elka.pw.sparkseq.statisticalTests._
import org.apache.spark.SparkContext
import pl.elka.pw.sparkseq.seqAnalysis.SparkSeqAnalysis
import pl.elka.pw.sparkseq.statisticalTests._
import org.apache.spark.SparkContext._
import collection.mutable.ArrayBuffer
import org.apache.hadoop.io.LongWritable
import fi.tkk.ics.hadoop.bam.{BAMInputFormat, SAMRecordWritable}
import pl.elka.pw.sparkseq.conversions.SparkSeqConversions
import scala.util.control._
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
    val bedFile = "Equus_caballus.EquCab2.73_exons_chr2.bed"
    val pathExonsList = rootPath+fileSplitSize.toString+"MB/aux/"+bedFile
    val genExonsMapB = sc.broadcast(SparkSeqConversions.BEDFileToHashMap(sc,pathExonsList ))


    val numTasks = 30

    val minRegLength= 10

    val caseIdFam1 = Array(38,39/*,42,44,45,47,53*/)
    val controlIdFam1 = Array(56,74/*,76,77,83,94*/)
    val caseIdFam2 = Array(100,111/*,29,36,52,55,64,69*/)
    val controlIdFam2 = Array(110,30/*,31,51,54,58,63,91,99*/)

    val caseSampSize = caseIdFam1.length + caseIdFam2.length + 1
    val controlSampSize = controlIdFam1.length + controlIdFam2.length  + 1

    val testSuff="_sort_chr1.bam"
    val chr = "chr1"
    val posStart=1
    val posEnd=500000
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
      .map(c=>if(c._1<0.001)(0.001,c._2) else if(c._1>=0.001 && c._1<0.01) (0.01,c._2) else if(c._1>=0.01 && c._1<0.05)(0.05,c._2) else (0.1,c._2) ) //make p-value discrete(OPTIMIZE!! it can be combined with getPval)!
      .groupByKey().sortByKey(true,8).map(r=>(r._1,r._2.sortBy(x=>x)) )
      .flatMap{r=>
          var regLenArray:ArrayBuffer[(Double,Int,Long)]=ArrayBuffer()
          var regStart = r._2(0)
          var regLength = 1
          var i = 1
          while(i<r._2.length){
          if(r._2(i)-1 != r._2(i-1) ){
            if(regLength>=minRegLength)
            regLenArray+=((r._1,regLength,regStart))
            regLength=1
          regStart=r._2(i)
          }
          else regLength+=1
            i=i+1
          }
          regLenArray.sortBy(-_._2)
    }
    //.flatMap(r=>r)
    //.flatMap(r=>(r._1,r._2 ) )
    .map(r=>(r._1,r._2,SparkSeqConversions.idToCoordinates(r._3)) )
    .map(r=>
      if(genExonsMapB.value.contains(r._3._1) ){
          val exons = genExonsMapB.value(r._3._1)
          var exId = 0
          var genId = 0
          val id = r._3._2/10000
          var exonOverlapPct = 0.0
          val loop = new Breaks
          loop.breakable{
               if(exons(id) != null){
               for(e<-exons(id)){
                 val exonIntersect = Range(r._3._2,r._3._2+r._2).intersect(Range(e._3,e._4))
                 if( exonIntersect.length>0 ){
                   exonOverlapPct = (exonIntersect.max-exonIntersect.min).toDouble/(e._4-e._3)
                   exId = e._2
                   genId = e._1
                   loop.break()
                  }
                }

               }
          }
        (r._1,r._2,r._3,"ENSECAG000000"+genId,exId,math.round(exonOverlapPct*10000).toDouble/10000)
        }
        else
          (r._1,r._2,r._3,"ChrNotFound",0,0.0)
      )

 .filter(r=>r._1<=0.01)
  val a =finalcovJoint.toArray()
  sc.stop()
  Thread.sleep(100)
  println("==========================================Results======================================")
  println("p-value".toString.padTo(10,' ')+"length".padTo(10, ' ')+"Coordinates".padTo(15, ' ')+"geneId".padTo(25,' ')+"exonId".padTo(10, ' ')+"exonOverlapPct")

  for(r<-a){
  println(r._1.toString.padTo(10,' ')+r._2.toString.padTo(10, ' ')+r._3.toString.padTo(15, ' ')+r._4.toString.padTo(25,' ')+r._5.toString.padTo(10, ' ')+r._6)
  }
    //println(rightCovJoin

  }
  //System.exit(0)
}
