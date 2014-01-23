/*** SimpleJob.scala ***/
package pl.elka.pw.sparkseq

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import pl.elka.pw.sparkseq.serialization.SparkSeqKryoProperties
import pl.elka.pw.sparkseq.util.SparkSeqContexProperties
import pl.elka.pw.sparkseq.conversions.SparkSeqConversions._
import pl.elka.pw.sparkseq.conversions.SparkSeqConversions
import pl.elka.pw.sparkseq.seqAnalysis.SparkSeqAnalysis

//import org.apache.spark.SparkConf
//import org.apache.spark._
import org.apache.spark.storage._
//import SparkContext._
//import spark._
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd._
import fi.tkk.ics.hadoop.bam.BAMInputFormat
import fi.tkk.ics.hadoop.bam.SAMRecordWritable
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import com.esotericsoftware.kryo.Kryo
import collection.mutable.ArrayBuffer
import pl.elka.pw.sparkseq.statisticalTests._
import java.util.HashMap
import scala.collection.mutable.Map
import com.github.tototoshi.csv._
import java.io.File 

object SparkSeqSimpleJob {

 def main(args: Array[String]) {


       SparkSeqContexProperties.setupContexProperties()
       SparkSeqKryoProperties.setupKryoContextProperties()
        //  val sparkConf = new SparkConf().setMaster("local[8]").setAppName("SparkSeq")
        //   val sc = new SparkContext("local","My app",sparkConf)
    //spark://hadoop-jobtracker001:7077
     val sc = new  SparkContext("local[8]", "sparkseq", "/opt/spark-0.9.0-incubating")
    // , List("target/scala-2.9.3/sparkseq_2.9.3-0.1.jar","/opt/hadoop-classpath/hadoop-bam-6.1-SNAPSHOT.jar","/opt/hadoop-classpath/picard-1.106.jar","/opt/hadoop-classpath/sam-1.106.jar", "/opt/hadoop-classpath/variant-1.106.jar","/opt/hadoop-classpath/tribble-1.106.jar","/opt/hadoop-classpath/commons-jexl-2.1.1.jar"))


    /*val seqAnalysisCase = new SparkSeqAnalysis(sc,"hdfs://hadoop-name001:9000/HG/16MB/HG00096.mapped.ILLUMINA.bwa.GBR.low_coverage.20120522.bam",1,"NULL",1,40)
    val out=seqAnalysisCase.getCoverageBase()
    out.persist(StorageLevel.MEMORY_ONLY_SER)
    println(out.count() )
    println(out.count() )
    */

    //val (initMax,initFree):(Long,Long) = sc.getExecutorMemoryStatus.values.take(1)
    val startTimeLoad = System.currentTimeMillis
    val CMTest = new SparkSeqCvM2STest()
    val fileSplitSize = 64

    val rootPath="/mnt/software/Phd_datastore/RAO/"
    val pathFam1 = rootPath+fileSplitSize.toString+"MB/condition_9/Fam1"
    val pathFam2 = rootPath+fileSplitSize.toString+"MB/condition_9/Fam2"
    val bedFile = "Equus_caballus.EquCab2.73_exons_chr2.bed"
    val pathExonsList = rootPath+fileSplitSize.toString+"MB/aux/"+bedFile
    //val pathExonsList="/hdfs_temp/Equus_caballus.EquCab2.73_exons_chr.bed"

    //val pathFam1 = "hdfs://hadoop-name001:9000/RAO/"+fileSplitSize.toString+"MB/condition_9/Fam1"
    //val pathFam2 = "hdfs://hadoop-name001:9000/RAO/"+fileSplitSize.toString+"MB/condition_9/Fam2"
    //val pathExonsList="hdfs://hadoop-name001:9000/RAO/conversions/Equus_caballus.EquCab2.73_exons_chr.bed"
    //val pathExonsList="/hdfs_temp/Equus_caballus.EquCab2.73_exons_chr.bed"

    // val cmDistTable = sc.textFile("hdfs://hadoop-name001:9000/RAO/cm8_7_2.txt").map(l => l.split("\t")).map(r=>(r.array(0).toDouble,r.array(1).toDouble) ).toArray



    val genExonsMapB = sc.broadcast(SparkSeqConversions.BEDFileToHashMap(sc,pathExonsList ))
    val numTasks = 30
    val caseIdFam1 = Array(38,39/*,42,44,45,47,53*/)
    val controlIdFam1 = Array(56,74/*,76,77,83,94*/)

    val caseIdFam2 = Array(100,111/*,29,36,52,55,64,69*/)
    val controlIdFam2 = Array(110,30/*,31,51,54,58,63,91,99*/)

    val normArray=Array(1.0,8622606.0/19357579.0,8622606.0/14087644.0,8622606.0/18824924.0,8622606.0/9651030.0,8622606.0/22731556.0,
                 8622606.0/15975604.0,8622606.0/17681528.0, 8622606.0/16323269.0,  8622606.0/18408612.0, 8622606.0/15934054.0, 8622606.0/22329258.0,
                 8622606.0/14788631.0, 8622606.0/14346120.0, 8622606.0/ 16693869.0)
    val minCount = 10
    val minRegLength= 10
    val minCoverageRatio = 0.33
    val pval = 0.1
    val caseSampSize = caseIdFam1.length + caseIdFam2.length + 1
    val controlSampSize = controlIdFam1.length + controlIdFam2.length  + 1

    val seqAnalysisCase = new SparkSeqAnalysis(sc,pathFam1+"/Case/Sample_25_sort.bam",25,1,numTasks)
    var id = 1
    for(i<-caseIdFam1){
        seqAnalysisCase.addBAM(sc,pathFam1+"/Case/Sample_"+i.toString+"*_sort.bam",i,1); id+=1}

    for(i<-caseIdFam2){
        seqAnalysisCase.addBAM(sc,pathFam2+"/Case/Sample_"+i.toString+"*_sort.bam",i,1); id+=1}



    val seqAnalysisControl = new SparkSeqAnalysis(sc,pathFam1+"/Control/Sample_26_sort.bam",26,1,numTasks)
    for(i<-controlIdFam1){
        seqAnalysisControl.addBAM(sc,pathFam1+"/Control/Sample_"+i.toString+"*_sort.bam",i,1 ); id+=1}

    for(i<-controlIdFam2){
        seqAnalysisControl.addBAM(sc,pathFam2+"/Control/Sample_"+i.toString+"*_sort.bam",i,1 ); id+=1}

    /*TEST Basecount */

    /*
    val covCase = seqAnalysisCase.getCoverageBase().map(r=>(r._1%100000000000L,r._2)).groupByKey()
    //	.map(c=> if(caseSampSize-c._2.length>0)(c._1,c._2++ArrayBuffer.fill[Int](caseSampSize-c._2.length)(0)) else (c._1,c._2) )
    //	.map(c=> if(mean(c._2) < 1 )(c._1,ArrayBuffer.fill[Int](caseSampSize)(0)) else(c._1,c._2)  )
    val covControl = seqAnalysisControl.getCoverageBase().map(r=>(r._1%100000000000L,r._2)).groupByKey()
    //	.map(c=> if(caseSampSize-c._2.length>0)(c._1,c._2++ArrayBuffer.fill[Int](caseSampSize-c._2.length)(0)) else (c._1,c._2) )
    //        .map(c=> if(mean(c._2) < 1 )(c._1,ArrayBuffer.fill[Int](caseSampSize)(0)) else(c._1,c._2)  )
    val covJoint = covCase.join(covControl)
      .map(c=>
            if(c._2._2 == null)
               (c._1,(c._2._1, ArrayBuffer.fill[Int](controlSampSize)(0)) )
      else if (c._2._1 == null)
               (c._1,(ArrayBuffer.fill[Int](caseSampSize)(0),c._2._2) )
      else
         (c._1,c._2) )
            .map(c=>if(c._2._1.length < caseSampSize || c._2._2.length < controlSampSize)
         (c._1,(c._2._1++Array.fill[Int](caseSampSize-c._2._1.length)(0),c._2._2++Array.fill[Int](controlSampSize-c._2._2.length)(0) ) )
      else
         (c._1,c._2)  )
    //	.map(r=>(r._1,r._2,CMTest.computeTestStat(r._2(0),r._2(1)) ) ).map(r=>(r._1,r._2,r._3,CMTest.getPValue(r._3,cmDistTable)) )
    println(covJoint.first())
    //cov.persist(StorageLevel.MEMORY_ONLY)
    //println(cov.count() )
    */


    /*Test Exon counts*/
    val covCase = seqAnalysisCase.getCoverageRegion(genExonsMapB).map(c=>(c._1%100000000000L,c._2) ).groupByKey()
      .map(c=>(c._1,c._2 )) //.partitionBy(new HashPartitioner(15))
    val covControl = seqAnalysisControl.getCoverageRegion(genExonsMapB).map(c=>(c._1%100000000000L,c._2) ).groupByKey()
      .map(c=>(c._1,c._2 ))//.partitionBy(new HashPartitioner(15))
    //val covJoint = covCase++covControl
    val covJoint = covCase.join(covControl)
    //val covCase = seqAnalysisCase.getCoverageRegion(genExonsMap)
    //val covControl = seqAnalysisControl.getCoverageRegion(genExonsMap)
    //val covUnion = seqAnalysisCase.getCoverageRegion(genExonsMap).union(seqAnalysisControl.getCoverageRegion(genExonsMap) )

    //val covJoint = covUnion.groupByKey(numTasks)
    //covCase.take(20).foreach(println)
    println("*****************")
    //covControl.take(20).foreach(println)
    //covJoint.take(20).foreach(println)

    println("*****************")
    //covJoint.saveAsTextFile("hdfs://hadoop-name001:9000/RAO/out1.txt")
    println(covJoint.count)
    //println(covCase.count+covControl.count)

    val endTimeLoad = System.currentTimeMillis

    //val (endMax,endFree):(Long,Long) = sc.getExecutorMemoryStatus.values.take(1)
    //val memSize = sc.getRDDStorageInfo(0).memSize
    //val diskSize = sc.getRDDStorageInfo(0).diskSize

    //println("########################After cache#########################")
    //val startTimeCache = System.currentTimeMillis
    //println(cov.count() )
    //val endTimeCache  = System.currentTimeMillis




    println("Summary:")
    println(("CacheLoadTime = %.3f secs").format((endTimeLoad-startTimeLoad)/1000.0) )
    //println(("CacheMemSize = %.3f GB").format((memSize)/(1024.0*1024.0*1024.0)) )
    //println(("CacheDiskSize = %.3f GB").format((diskSize)/(1024.0*1024.0*1024.0)) )
    //println(("CacheHitTime = %.5f secs").format((endTimeCache-startTimeCache)/1000.0) )

	
	

}
}
