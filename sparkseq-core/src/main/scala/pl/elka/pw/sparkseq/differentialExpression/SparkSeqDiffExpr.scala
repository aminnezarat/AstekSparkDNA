/**
 * Copyright (c) 2014. [insert your company or name here]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pl.elka.pw.sparkseq.differentialExpression

import org.apache.spark.{HashPartitioner, SparkContext, RangePartitioner}
import org.apache.spark.SparkContext._
import pl.elka.pw.sparkseq.seqAnalysis.SparkSeqAnalysis
import org.apache.spark.rdd._
import scala.collection.mutable.ArrayBuffer
import pl.elka.pw.sparkseq.statisticalTests._
import pl.elka.pw.sparkseq.conversions.SparkSeqConversions
import scala.util.control._
import com.github.nscala_time.time.Imports._
import java.io._

/**
 * Created by mwiewior on 2/24/14.
 */
/**
 *
 * @param iSC Apache Spark context.
 * @param iSeqAnalCase SparkSeqAnalysis object for case samples.
 * @param iSeqAnalControl SparkSeqAnalysis object for control samples.
 * @param iBEDFile Filepath to BED-like file with exon annotations.
 * @param iChr Chromosome (eg. chr1)
 * @param iStartPos Starting position in a chromosome (default 1).
 * @param iEndPos End position in a chromosome (default 300000000).
 * @param iMinCoverage Minimal base-coverage (default 10).
 * @param iMinRegionLen Minimal region length (default 2).
 * @param iMaxPval Maximum p-value for base differential expression (default 0.05).
 * @param iNumTasks Number of tasks and partitions (default 8).
 * @param iNumReducers Number of reducer workers (default 8).
 * @param confDir Configuration directory.
 */
class SparkSeqDiffExpr(iSC: SparkContext, iSeqAnalCase: SparkSeqAnalysis, iSeqAnalControl: SparkSeqAnalysis, iBEDFile: String, iChr: String = "*",
                       iStartPos: Int = 1, iEndPos: Int = 300000000, iMinCoverage: Int = 10, iMinRegionLen: Int = 2,
                       iMaxPval: Double = 0.1, iNumTasks: Int = 8, iNumReducers: Int = 8, confDir: String) extends Serializable {

  private val caseSampleNum: Int = iSeqAnalCase.sampleNum
  private val controlSampleNum: Int = iSeqAnalControl.sampleNum
  var diffExprRDD: RDD[(Double, Int, (String, Int), Double, String, Int, Double)] = new EmptyRDD[(Double, Int, (String, Int), Double, String, Int, Double)](iSC)
  val cmDistTable = iSC.textFile(confDir + "cm" + caseSampleNum + "_" + controlSampleNum + "_2.txt")
    .map(l => l.split("\t"))
    .map(r => (r.array(0).toDouble, r.array(1).toDouble))
    .toArray
  val cmDistTableB = iSC.broadcast(cmDistTable)
  val genExonsMapB = iSC.broadcast(SparkSeqConversions.BEDFileToHashMap(iSC, confDir + iBEDFile))

  private def groupSeqAnalysis(iSeqAnalysis: SparkSeqAnalysis, iSampleNum: Int): RDD[(Long, Seq[Int])] = {
    val seqGrouped = iSeqAnalysis.getCoverageBaseRegion(iChr, iStartPos, iEndPos)
      .map(r => (r._1 % 1000000000000L, r._2))
      .groupByKey()
      .mapValues(c => if ((iSampleNum - c.length) > 0) (c ++ ArrayBuffer.fill[Int](iSampleNum - c.length)(0)) else (c))
    return (seqGrouped)
  }

  private def joinSeqAnalysisGroup(iSeqAnalysisGroup1: RDD[(Long, Seq[Int])], iSeqAnalysisGroup2: RDD[(Long, Seq[Int])]): RDD[(Long, (Seq[Int], Seq[Int]))] = {
    val seqJoint: RDD[(Long, (Seq[Seq[Int]], Seq[Seq[Int]]))] = iSeqAnalysisGroup1.cogroup(iSeqAnalysisGroup2)
    val finalSeqJoint = seqJoint
      // .mapValues(r=>(r._1(0),r._2(0)))
      .mapValues(r =>
      (if (r._1.length == 0) ArrayBuffer.fill[Int](caseSampleNum)(0) else r._1(0),
        if (r._2.length == 0) ArrayBuffer.fill[Int](controlSampleNum)(0) else r._2(0))
      )
    return (finalSeqJoint)
  }

  private def computeTwoSampleCvMTest(iSeqCC: RDD[(Long, (Seq[Int], Seq[Int]))]): RDD[((Int, Double), (Long, Double))] = {

    val twoSampleTests = iSeqCC
      .map(r => (r._1, r._2, SparkSeqCvM2STest.computeTestStat(r._2._1, r._2._2)))
      .map(r => ((r._1), (r._2, r._3, SparkSeqCvM2STest.getPValue(r._3, cmDistTableB), SparkSeqStats.mean(r._2._1) / SparkSeqStats.mean(r._2._2))))
      .map(r => (((r._1 / 1000000000L).toInt, r._2._3), (r._1, r._2._4)))
      .filter(r => r._1._2 <= iMaxPval)
    return (twoSampleTests)
  }

  private def findContRegionsEqual(iSeq: RDD[((Int, Double), Seq[(Long, Double)])]): RDD[(Double, Int, (String, Int), Double, String, Int, Double)] = {
    val iSeqPart = iSeq.map(r => (r._1._2, r._2.sortBy(_._1)))
      .partitionBy(new HashPartitioner(iNumTasks * 3))
    iSeqPart
      .mapPartitions {
      partitionIterator =>
        var regLenArray = new Array[(Double, Int, (String, Int), Double, String, Int, Double)](1000000)
        var k = 0
        for (r <- partitionIterator) {
          var regStart = r._2(0)._1
          var regLength = 1
          var fcSum = 0.0
          var i = 1
          while (i < r._2.length) {
            if (r._2(i)._1 - 1 != r._2(i - 1)._1) {
              if (regLength >= iMinRegionLen) {
                regLenArray(k) = (mapRegionsToExons((r._1, regLength, regStart, fcSum / regLength)))
                k += 1
              }
              regLength = 1
              fcSum = 0.0
              regStart = r._2(i)._1

            }
            else {
              regLength += 1
              fcSum += (r._2(i)._2)
            }
            i = i + 1
          }
        }
        Iterator(regLenArray.filter(r => r != null).sortBy(-_._2))
    }.flatMap(r => r)
  }

  private def findContRegionsLessEqual() = {}

  private def getRangeIntersect(r1Start: Int, r1End: Int, r2Start: Int, r2End: Int): (Int, Int) = {
    val maxStart = math.max(r1Start, r2Start)
    val minEnd = math.min(r1End, r2End)
    (maxStart, minEnd)
  }

  private def mapRegionsToExons(r: (Double, Int, Long, Double)): (Double, Int, (String, Int), Double, String, Int, Double) = {

    val reg = (r._1, r._2, SparkSeqConversions.idToCoordinates(r._3), r._4)
    if (genExonsMapB.value.contains(reg._3._1)) {
      val exons = genExonsMapB.value(reg._3._1)
      var exId = 0
        var genId = ""
      val id = reg._3._2 / 10000
      var exonOverlapPct = 0.0
        val loop = new Breaks
        loop.breakable {
          if (exons(id) != null) {
            for (e <- exons(id)) {
              val exonIntersect = getRangeIntersect(reg._3._2, reg._3._2 + reg._2, e._3, e._4)
              val exonIntersectLen = exonIntersect._2 - exonIntersect._1
              if (exonIntersectLen > 0) {
                exonOverlapPct = exonIntersectLen.toDouble / (e._4 - e._3)
                exId = e._2
                genId = e._1
                //loop.break() //because there are some overlapping regions
              }
            }

          }
        }
      (reg._1, reg._2, reg._3, reg._4, genId, exId, math.round(exonOverlapPct * 10000).toDouble / 10000)
    }
      else
        (reg._1, reg._2, reg._3, reg._4, "ChrNotFound", 0, 0.0)

  }

  /**
   *
   * @return RDD of tuples(p-value,regionLength, (chrom,starPosition),foldChange,genId,exonId,exonRegionOverlap)
   */
  def computeDiffExpr(): RDD[(Double, Int, (String, Int), Double, String, Int, Double)] = {

    val seqGroupCase = groupSeqAnalysis(iSeqAnalCase, caseSampleNum)
    val seqGroupControl = groupSeqAnalysis(iSeqAnalControl, controlSampleNum)
    val seqJointCC = joinSeqAnalysisGroup(seqGroupCase, seqGroupControl)
    val seqFilterCC = seqJointCC.filter(r => (SparkSeqStats.mean(r._2._1) > iMinCoverage || SparkSeqStats.mean(r._2._2) > iMinCoverage))
    val seqCompTest = computeTwoSampleCvMTest(seqFilterCC)
    val seqPValGroup = seqCompTest
      .groupByKey()
    val seqReg = findContRegionsEqual(seqPValGroup)
      .map(r => (r._1, r._2, r._3, if (r._4 < 1.0) -1 / r._4; else r._4, r._5, r._6, r._7))
    diffExprRDD = seqReg
    return (seqReg)
  }

  private def fetchReults(num: Int): Array[(Double, Int, (String, Int), Double, String, Int, Double)] = {
    val results = diffExprRDD.coalesce(1).takeOrdered(num)(Ordering[(Double, Double, Int)]
      .on(r => (r._1, -(math.abs(r._4)), -r._2)))
    Thread.sleep(100)
    return (results)
  }

  /**
   *
   * @param iNum Number of top regions sorted  by p-value asc, foldChange desc and region length desc to be printed (default 10000).
   */
  def printResults(iNum: Int = 10000) = {

    val a = fetchReults(iNum)
    val header = "p-value".toString.padTo(10, ' ') + "foldChange".padTo(15, ' ') + "length".padTo(10, ' ') +
      "Coordinates".padTo(20, ' ') + "geneId".padTo(25, ' ') + "exonId".padTo(10, ' ') + "exonOverlapPct"
    println("=======================================Results======================================")
    println(header)

    for (r <- a) {
      val rec = (math.round(r._1 * 100000).toDouble / 100000).toString.padTo(10, ' ') + (math.round(r._4 * 10000).toDouble / 10000).toString.padTo(15, ' ') +
        r._2.toString.padTo(10, ' ') + r._3.toString.padTo(20, ' ') + r._5.toString.padTo(25, ' ') + r._6.toString.padTo(10, ' ') + r._7
      println(rec)
    }

  }

  /**
   *
   * @param iNum Number of top regions sorted  by p-value asc, foldChange desc and region length desc to be saved to file (default 10000).
   * @param iFilePathLacal Local path to save top iNum regions locally.
   * @param iFilePathRemote Remote path to HDFS storage to save all the results.
   */
  def saveResults(iNum: Int = 10000, iFilePathLacal: String = "sparkseq_10000.txt", iFilePathRemote: String) = {
    if (iNum <= 10000) {
      val a = fetchReults(iNum)
      val writer = new PrintWriter(new File(iFilePathLacal))
      val header = "p-value".toString.padTo(10, ' ') + "foldChange".padTo(15, ' ') + "length".padTo(10, ' ') +
        "Coordinates".padTo(20, ' ') + "geneId".padTo(25, ' ') + "exonId".padTo(10, ' ') + "exonOverlapPct"
      println("=======================================Results======================================")
      writer.write(header + "\n")
      for (r <- a) {
        var rec = (math.round(r._1 * 100000).toDouble / 100000).toString.padTo(10, ' ') + (math.round(r._4 * 10000).toDouble / 10000).toString.padTo(25, ' ') +
          r._2.toString.padTo(10, ' ') + r._3.toString.padTo(20, ' ') + r._5.toString.padTo(25, ' ') + r._6.toString.padTo(10, ' ') + r._7
        writer.write(rec + "\n")
      }
      writer.close()
    }
    diffExprRDD.saveAsTextFile(iFilePathRemote)
  }

}
