package pl.elka.pw.sparkseq
import pl.elka.pw.sparkseq.statisticalTests._
import pl.elka.pw.sparkseq.seqAnalysis.SparkSeqAnalysis
import pl.elka.pw.sparkseq.statisticalTests._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import collection.mutable.ArrayBuffer
import org.apache.hadoop.io.LongWritable
import fi.tkk.ics.hadoop.bam.{BAMInputFormat, SAMRecordWritable}
import pl.elka.pw.sparkseq.conversions.SparkSeqConversions
import scala.util.control._
import pl.elka.pw.sparkseq.util.SparkSeqContexProperties
import pl.elka.pw.sparkseq.serialization.SparkSeqKryoProperties
import scala.Array
import org.apache.spark.RangePartitioner
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import java.io._
import com.github.nscala_time.time._
import com.github.nscala_time.time.Imports._
/**
 * Created by marek on 2/8/14.
 */
object SparkSeqBaseDE {

  def main(args: Array[String]) {

    SparkSeqContexProperties.setupContexProperties()
    SparkSeqKryoProperties.setupKryoContextProperties()
    val conf = new SparkConf()
      .setMaster("mesos://sparkseq001.cloudapp.net:5050")
      .setAppName("SparkSeq")
      .set("spark.executor.uri", "hdfs:///frameworks/spark/0.9.0/spark-0.9.0-incubating-hadoop_1.2.1-bin.tar.gz")
      .setJars(Seq(System.getenv("ADD_JARS")))
      .setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)

   //val sc = new  SparkContext("spark://sparkseq001.cloudapp.net:7077"/*"local[4]"*/, "sparkseq", System.getenv("SPARK_HOME"),  Seq(System.getenv("ADD_JARS")))

    val timeStamp=DateTime.now.toString()
    val fileSplitSize = 64
    val rootPath="hdfs://sparkseq002.cloudapp.net:9000/BAM/"

    /*val pathFam1 = rootPath+fileSplitSize.toString+"MB/condition_9/Fam1"
    val pathFam2 = rootPath+fileSplitSize.toString+"MB/condition_9/Fam2"
    val bedFile = "Equus_caballus.EquCab2.74_exons_chr_ordered.bed"
    val pathExonsList = rootPath+fileSplitSize.toString+"MB/aux/"+bedFile
     val caseIdFam1 = Array(38,39,42,44,45,47,53)
    val controlIdFam1 = Array(56,74,76,77,83,94)
    val caseIdFam2:Array[Int] = Array(100,111,29,36,52,55,64,69)
    //val controlIdFam2:Array[Int] = Array(110,30,31,51,54,58,63,91,99)
    //val testSuff="_sort.bam"
    */

    val pathFam1 = rootPath+fileSplitSize.toString+"MB/GSE51403"
    val pathFam2 = rootPath+fileSplitSize.toString+"MB/GSE51403"
    val bedFile = "Homo_sapiens.GRCh37.74_exons_chr_ordered.bed"
    val pathExonsList = rootPath+fileSplitSize.toString+"MB/aux/"+bedFile


    val genExonsMapB = sc.broadcast(SparkSeqConversions.BEDFileToHashMap(sc,pathExonsList ))
    val numTasks = 16
    val numPartitions = 24




    /*val caseIdFam1 = Array(950080,950082,950084,950086)
    val controlIdFam1 = Array(950081,950083,950085,950087)*/
    val caseIdFam1 = Array(1012920,1012922,1012924,1012927,1012930,1012933)
    val controlIdFam1 = Array(1012938,1012942,1012945,1012948,1012951,1012954)

    val caseIdFam2:Array[Int] = Array()
    val controlIdFam2:Array[Int] = Array()

    val caseSampSize = caseIdFam1.length + caseIdFam2.length + 1
    val controlSampSize = controlIdFam1.length + controlIdFam2.length  + 1


    val testSuff="_sort.bam"
    val chr = args(0)
    val posStart=1
    val posEnd=300000000
    val minAvgBaseCov = 10
    val minPval = 0.05
    val minRegLength= 10

    //./XCVMTest 7 7 | cut -f2,4 | sed 's/^\ \ //g' | grep "^[[:digit:]]" >cm7_7_2.txt
    val cmDistTable = sc.textFile(rootPath+fileSplitSize.toString+"MB/aux/cm"+caseSampSize+"_"+controlSampSize+"_2.txt")
      .map(l => l.split("\t"))
      .map(r=>(r.array(0).toDouble,r.array(1).toDouble) )
      .toArray
    val cmDistTableB = sc.broadcast(cmDistTable)


    /*val normArray=Array(1.0,8622606.0/19357579.0,8622606.0/14087644.0,8622606.0/18824924.0,8622606.0/9651030.0,8622606.0/22731556.0,
      8622606.0/15975604.0,8622606.0/17681528.0, 8622606.0/16323269.0,  8622606.0/18408612.0, 8622606.0/15934054.0, 8622606.0/22329258.0,
      8622606.0/14788631.0, 8622606.0/14346120.0, 8622606.0/ 16693869.0)
*/

    val seqAnalysisCase = new SparkSeqAnalysis(sc,pathFam1+"/Case/Sample_1012918"+testSuff,25,1,numTasks)
    val bamFileCountCaseFirst= sc.newAPIHadoopFile[LongWritable,SAMRecordWritable,BAMInputFormat](pathFam1+"/Case/Sample_1012918"+testSuff).count()
    for(i<-caseIdFam1++caseIdFam2){
      var path:String = ""
      if(caseIdFam1.contains(i))
        path = pathFam1+"/Case/Sample_"+i.toString+testSuff
      else
        path = pathFam2+"/Case/Sample_"+i.toString+testSuff
      val bamFileCount= sc.newAPIHadoopFile[LongWritable,SAMRecordWritable,BAMInputFormat](path).count()
      seqAnalysisCase.addBAM(sc,path,i,bamFileCountCaseFirst.toDouble/bamFileCount.toDouble)
    }



    val seqAnalysisControl = new SparkSeqAnalysis(sc,pathFam1+"/Control/Sample_1012936"+testSuff,26,1,numTasks)
    val bamFileCountControlFirst= sc.newAPIHadoopFile[LongWritable,SAMRecordWritable,BAMInputFormat](pathFam1+"/Control/Sample_1012936"+testSuff).count()
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
      .map(r=>(r._1%1000000000000L,r._2))
      .groupByKey()
      .mapValues(c=> if((caseSampSize-c.length)>0)(c++ArrayBuffer.fill[Int](caseSampSize-c.length)(0)) else (c) )
    //val covCasePart =   covCase.partitionBy(new HashPartitioner(numPartitions))

          //.filter(r=>(SparkSeqStats.mean(r._2) > minAvgBaseCov && r._2.length > caseSampSize/2) )

    val covControl = seqAnalysisControl.getCoverageBaseRegion(chr,posStart,posEnd)
      .map(r=>(r._1%1000000000000L,r._2))
      .groupByKey()
      .mapValues(c=> if((controlSampSize-c.length)>0)(c++ArrayBuffer.fill[Int](controlSampSize-c.length)(0)) else (c) )
    //val covControlPart =   covControl.partitionBy(new HashPartitioner(numPartitions))
        //.filter(r=>(SparkSeqStats.mean(r._2) > minAvgBaseCov && r._2.length > controlSampSize/2) )


    val leftCovJoint = covCase.leftOuterJoin(covControl)
      //.subtract(covCase.join(covControl.map(r=>(r._1,Option(r._2)) ) ) )
    val rightCovJoint = covCase.rightOuterJoin(covControl)
   println(leftCovJoint.count() )
    println(rightCovJoint.count() )

//println(leftCovJoint)
    //final join + compute Cramver von Mises test statistics + filtering
    val finalcovJoint = leftCovJoint.map(r=>(r._1,Option(r._2._1),r._2._2)).union(rightCovJoint.map(r=>(r._1,r._2._1,Option(r._2._2))) ).map(r=>(r._1,(r._2,r._3)))
        .map( r=> (r._1,(r._2._1 match {case Some(x) =>x;case None =>ArrayBuffer.fill[Int](caseSampSize)(0)},
                         r._2._2 match {case Some(x) =>x;case None =>ArrayBuffer.fill[Int](controlSampSize)(0)}) ) ).distinct()

      .filter(r=> ( SparkSeqStats.mean(r._2._1) > minAvgBaseCov || SparkSeqStats.mean(r._2._2) > minAvgBaseCov ) )
      .map(r=>(r._1,r._2,SparkSeqCvM2STest.computeTestStat(r._2._1,r._2._2) ) )
      .map(r=>((r._1),(r._2,r._3,SparkSeqCvM2STest.getPValue(r._3,cmDistTableB ),SparkSeqStats.mean(r._2._1)/SparkSeqStats.mean(r._2._2)))  )
      .map(r=>(r._2._3,(r._1,r._2._4))) //pick position and p-value
      //.map(r=>(r._1,r._2,SparkSeqConversions.idToCoordinates(r._2)) )

      .map(c=>if(c._1<0.001)(0.001,c._2) else if(c._1>=0.001 && c._1<0.01) (0.01,c._2) else if(c._1>=0.01 && c._1<0.05)(0.05,c._2) else (0.1,c._2) ) //make p-value discrete(OPTIMIZE!! it can be combined with getPval)!
      .filter(r=>r._1<=minPval)
    //println(finalcovJoint.count())
     //.groupByKey()
      .groupByKey(numTasks)
     val f = finalcovJoint.partitionBy(new RangePartitioner[Double,Seq[(Long,Double)]](4,finalcovJoint))
     .map(r=>(r._1,r._2.sortBy(_._1).distinct)).map(r=>(r._1,r._2.distinct) ) //2*x distinct workaround
     //  println(f.first.toString)
      .mapPartitions{partitionIterator =>
          var regLenArray:ArrayBuffer[(Double,Int,Long,Double)]=ArrayBuffer()
       for (r <- partitionIterator){
          var regStart = r._2(0)._1
          var regLength = 1
          var fcSum = 0.0
          var i = 1
          while(i<r._2.length){
          if(r._2(i)._1-1 != r._2(i-1)._1 ){
            if(regLength>=minRegLength)
            regLenArray+=((r._1,regLength,regStart,fcSum/regLength))
          regLength=1
          fcSum = 0.0
          regStart=r._2(i)._1
          }
          else{
            regLength+=1
            fcSum+=(r._2(i)._2)
          }
            i=i+1
          }
       }
          Iterator(regLenArray.sortBy(-_._2) )
    }.flatMap(r=>r)
    //.flatMap(r=>r)
    //.flatMap(r=>(r._1,r._2 ) )
   .map(r=>(r._1,r._2,SparkSeqConversions.idToCoordinates(r._3),r._4) )
    .map(r=>
      if(genExonsMapB.value.contains(r._3._1) ){
          val exons = genExonsMapB.value(r._3._1)
          var exId = 0
          var genId = ""
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
                   //loop.break() //because there are some overlapping regions
                  }
                }

               }
          }
        (r._1,r._2,r._3,r._4,genId,exId,math.round(exonOverlapPct*10000).toDouble/10000)
        }
        else
          (r._1,r._2,r._3,r._4,"ChrNotFound",0,0.0)
      )


  val a =f.toArray()
    .map(r=>(r._1,r._2,r._3,if(r._4<1.0) -1/r._4;else r._4,r._5,r._6,r._7))
    .sortBy(r=>(r._1,-(math.abs(r._4)),-r._2))
//    val b = finalcovJoint.take(10)
 //   b.foreach(println)
  sc.stop()
  Thread.sleep(100)
  val writer = new PrintWriter(new File(timeStamp+"exp.txt" ))
  val header="p-value".toString.padTo(10,' ')+"foldChange".padTo(15, ' ')+"length".padTo(10, ' ')+"Coordinates".padTo(20, ' ')+"geneId".padTo(25,' ')+"exonId".padTo(10, ' ')+"exonOverlapPct"
  println("=======================================Results======================================")
  println(header)
  writer.write(header+"\n")

  for(r<-a){
    var rec = r._1.toString.padTo(10,' ')+(math.round(r._4*10000).toDouble/10000).toString.padTo(15, ' ')+r._2.toString.padTo(10, ' ')+r._3.toString.padTo(20, ' ')+r._5.toString.padTo(25,' ')+r._6.toString.padTo(10, ' ')+r._7
    println(rec)
    writer.write(rec+"\n")
  }
  writer.close()
    //println(rightCovJoin

  }
  //System.exit(0)
}
