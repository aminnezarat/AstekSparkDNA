package pl.elka.pw.sparkseq

/**
 * Created by mesos on 3/8/14.
 */

import pl.elka.pw.sparkseq.differentialExpression.SparkSeqDiffExpr
import pl.elka.pw.sparkseq.seqAnalysis.SparkSeqAnalysis
import pl.elka.pw.sparkseq.serialization.SparkSeqKryoProperties
import pl.elka.pw.sparkseq.statisticalTests._
import pl.elka.pw.sparkseq.seqAnalysis.SparkSeqAnalysis
import pl.elka.pw.sparkseq.statisticalTests._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import collection.mutable.ArrayBuffer
import org.apache.hadoop.io.LongWritable
import fi.tkk.ics.hadoop.bam.{BAMInputFormat, SAMRecordWritable}
import pl.elka.pw.sparkseq.conversions.SparkSeqConversions
import pl.elka.pw.sparkseq.util.SparkSeqContexProperties
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
import pl.elka.pw.sparkseq.differentialExpression.SparkSeqDiffExpr

/**
 * Created by marek on 2/8/14.
 */
object SparkSeqDERFinder {

  def main(args: Array[String]) {

    SparkSeqContexProperties.setupContexProperties()
    SparkSeqKryoProperties.setupKryoContextProperties()
    val conf = new SparkConf()
      .setMaster("mesos://sparkseq001.cloudapp.net:5050")
      .setAppName("SparkSeq")
      .set("spark.executor.uri", "hdfs:///frameworks/spark/0.9.0/spark-0.9.0-incubating-hadoop_1.2.1-bin.tar.gz")
      .setJars(Seq(System.getenv("ADD_JARS")))
      .setSparkHome(System.getenv("SPARK_HOME"))
      .set("spark.mesos.coarse", "true")
    //.set("spark.cores.max", "24")
    val sc = new SparkContext(conf)

    //val sc = new  SparkContext("spark://sparkseq001.cloudapp.net:7077"/*"local[4]"*/, "sparkseq", System.getenv("SPARK_HOME"),  Seq(System.getenv("ADD_JARS")))

    val timeStamp = DateTime.now.toString()
    val fileSplitSize = 64
    val rootPath = "hdfs://sparkseq002.cloudapp.net:9000/BAM/"

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

    val pathFam1 = rootPath + fileSplitSize.toString + "MB/derfinder/chrom_Y"
    val pathFam2 = rootPath + fileSplitSize.toString + "MB/derfinder/chrom_Y"
    //val bedFile = "Homo_sapiens.GRCh37.74_exons_chr_merged_id_st.bed"
    val bedFile = "Homo_sapiens.GRCh37.74_exons_chr_sort_uniq_id.bed"
    val pathExonsList = rootPath + fileSplitSize.toString + "MB/aux/" + bedFile


    //val genExonsMapB = sc.broadcast(SparkSeqConversions.BEDFileToHashMap(sc,pathExonsList ))
    val numTasks = 16
    val numPartitions = 24




    /*val caseIdFam1 = Array(950080,950082,950084,950086)
    val controlIdFam1 = Array(950081,950083,950085,950087)*/
    val caseIdFam1 = Array(11, 32, 3, 42, 43, 47, 53, 58)
    val controlIdFam1 = Array(23, 33, 40, 55, 56)

    val caseIdFam2: Array[Int] = Array()
    val controlIdFam2: Array[Int] = Array()

    val caseSampSize = caseIdFam1.length + caseIdFam2.length + 1
    val controlSampSize = controlIdFam1.length + controlIdFam2.length + 1


    val testSuff = "_Y.bam"
    val chr = args(0)
    val posStart = 1
    val posEnd = 300000000
    val minAvgBaseCov = 10
    val minPval = 0.05
    val minRegLength = 10


    val seqAnalysisCase = new SparkSeqAnalysis(sc, pathFam1 + "/M/orbFrontalF1" + testSuff, 25, 1, numTasks)
    val bamFileCountCaseFirst = sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](pathFam1 + "/M/orbFrontalF1" + testSuff).count()
    for (i <- caseIdFam1 ++ caseIdFam2) {
      var path: String = ""
      if (caseIdFam1.contains(i))
        path = pathFam1 + "/M/orbFrontalF" + i.toString + testSuff
      else
        path = pathFam2 + "/M/orbFrontalF" + i.toString + testSuff
      val bamFileCount = sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](path).count()
      seqAnalysisCase.addBAM(sc, path, i, bamFileCountCaseFirst.toDouble / bamFileCount.toDouble)
    }



    val seqAnalysisControl = new SparkSeqAnalysis(sc, pathFam1 + "/F/orbFrontalF2" + testSuff, 26, 1, numTasks)
    val bamFileCountControlFirst = sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](pathFam1 + "/F/orbFrontalF2" + testSuff).count()
    for (i <- controlIdFam1 ++ controlIdFam2) {
      var path: String = ""
      if (controlIdFam1.contains(i))
        path = pathFam1 + "/F/orbFrontalF" + i.toString + testSuff
      else
        path = pathFam2 + "/F/orbFrontalF" + i.toString + testSuff
      val bamFileCount = sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](path).count()
      seqAnalysisControl.addBAM(sc, path, i, bamFileCountControlFirst.toDouble / bamFileCount.toDouble)

    }
    /*    val seqGrouped = seqAnalysisControl.getCoverageBaseRegion("chr21", 40000000 , 50000000)
          .map(r => (r._1 % 1000000000000L, r._2))
          .groupByKey()
          .mapValues(c => if ((7 - c.length) > 0) (c ++ ArrayBuffer.fill[Int](7 - c.length)(0)) else (c))
          .filter(r=>r._1>=21043782390L && r._1<=21043782490L)
          .sortByKey()
          .toArray()
           for(a<-seqGrouped)
             println(a)*/
    val minRegLen = args(1).toInt
    val maxPval = args(2).toDouble
    val diffExp = new SparkSeqDiffExpr(sc, seqAnalysisCase, seqAnalysisControl,
      iChr = args(0).mkString, confDir = rootPath + fileSplitSize.toString + "MB/aux/", iNumTasks = 24, iBEDFile = bedFile, iMaxPval = maxPval, iMinRegionLen = minRegLen, iMinCoverage = 1)
    val t = diffExp.computeDiffExpr(iCoalesceRegDiffPVal = true)
    diffExp.saveResults(iFilePathRemote = "hdfs://sparkseq002.cloudapp.net:9000/BAM/sparkseq_DERF_" + minRegLen.toString + "_" + args(0).replace("*", "whole").mkString + "_" + maxPval.toString + ".txt",
      iFilePathLocal = "sparkseq_local_DERF_" + minRegLen.toString + "_" + args(0).replace("*", "whole").mkString + "_" + maxPval.toString + ".txt")
    //System.exit(0
    sc.stop()

  }
}
