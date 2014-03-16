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

import org.apache.spark.{HashPartitioner, SparkContext, RangePartitioner, broadcast}
import org.apache.spark.SparkContext._
import pl.elka.pw.sparkseq.seqAnalysis.SparkSeqAnalysis
import org.apache.spark.rdd._
import scala.collection.mutable.ArrayBuffer
import pl.elka.pw.sparkseq.statisticalTests._
import pl.elka.pw.sparkseq.conversions.SparkSeqConversions
import scala.util.control._
import com.github.nscala_time.time.Imports._
import java.io._
import pl.elka.pw.sparkseq.permutationTests._
import pl.elka.pw.sparkseq.statisticalTests._

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
 * @param iMaxPval Maximum p-value for base differential expression (default 0.1).
 * @param iNumTasks Number of tasks and partitions (default 8).
 * @param iNumReducers Number of reducer workers (default 8).
 * @param confDir Configuration directory.
 */
class SparkSeqDiffExpr(iSC: SparkContext, iSeqAnalCase: SparkSeqAnalysis, iSeqAnalControl: SparkSeqAnalysis, iBEDFile: String, iChr: String = "*",
                       iStartPos: Int = 1, iEndPos: Int = 300000000, iMinCoverage: Double = 1.0, iMinRegionLen: Int = 2,
                       iMaxPval: Double = 0.1, iNumTasks: Int = 8, iNumReducers: Int = 8, confDir: String) extends Serializable {

  private val maxPval = iMaxPval
  private val chrName = iChr
  private val minRegLen = iMinRegionLen
  private val minExonPct = 0.0
  private var coalesceRegDiffPVal = false

  private val caseSampleNum: Int = iSeqAnalCase.sampleNum
  private val controlSampleNum: Int = iSeqAnalControl.sampleNum
  private var seqRegDERDDExons: RDD[(Int, (Double, Int, (String, Int), Double, String, Int, Double, Double, Double, Double))] = _
  private var seqRegDERDDPhase1: RDD[(Double, Int, (String, Int), Double, String, Int, Double, Double, Double, Int)] = _
  private var seqRegDERDDPhase2: RDD[(Double, Int, (String, Int), Double, String, Int, Double, Double, Double, Int)] = _
  //= new EmptyRDD[(Double, Int, (String, Int), Double, String, Int, Double,Double,Double)](iSC)
  private var seqRegContDERDD: RDD[(Double, Int, (String, Int), Double, String, Int, Double, Double, Double, Int)] = _
  //new EmptyRDD[(Double, Int, (String, Int), Double, String, Int, Double,Double,Double)](iSC)
  private val cmDistTable = iSC.textFile(confDir + "cm" + caseSampleNum + "_" + controlSampleNum + "_2.txt")
    .map(l => l.split("\t"))
    .map(r => (r.array(0).toDouble, r.array(1).toDouble))
    .toArray
  private val cmDistTableB = iSC.broadcast(cmDistTable)
  private val genExonsMapB = iSC.broadcast(SparkSeqConversions.BEDFileToHashMap(iSC, confDir + iBEDFile))
  private val genExonsMapLookupB = iSC.broadcast(SparkSeqConversions.BEDFileToHashMapGeneExon(iSC, confDir + iBEDFile))

  private def groupSeqAnalysis(iSeqAnalysis: SparkSeqAnalysis, iSampleNum: Int): RDD[(Long, Seq[Int])] = {
    val seqGrouped = iSeqAnalysis.getCoverageBaseRegion(iChr, iStartPos, iEndPos)
      .map(r => (r._1 % 1000000000000L, r._2))
      .groupByKey()
      .mapValues(c => if ((iSampleNum - c.length) > 0) (c ++ ArrayBuffer.fill[Int](iSampleNum - c.length)(0)) else (c))
    return (seqGrouped)
  }

  private def joinSeqAnalysisGroup(iSeqAnalysisGroup1: RDD[(Long, Seq[Int])], iSeqAnalysisGroup2: RDD[(Long, Seq[Int])]): RDD[(Long, (Seq[Int], Seq[Int]))] = {
    /*  val sAnalysisG1 = iSeqAnalysisGroup1.partitionBy(new RangePartitioner[Long, Seq[Int]](72, iSeqAnalysisGroup1))
      val sAnalysisG2 = iSeqAnalysisGroup2.partitionBy(new RangePartitioner[Long, Seq[Int]](72, iSeqAnalysisGroup2))
      val seqJoint = sAnalysisG1.cogroup(sAnalysisG2)*/
    val seqJoint: RDD[(Long, (Seq[Seq[Int]], Seq[Seq[Int]]))] = iSeqAnalysisGroup1.cogroup(iSeqAnalysisGroup2)
    val finalSeqJoint = seqJoint
      // .mapValues(r=>(r._1(0),r._2(0)))
      .map(r => (r._1,
      (if (r._2._1.length == 0) ArrayBuffer.fill[Int](caseSampleNum)(0) else r._2._1(0),
        if (r._2._2.length == 0) ArrayBuffer.fill[Int](controlSampleNum)(0) else r._2._2(0)))
      )
    return (finalSeqJoint)
  }

  private def joinSeqAnalysisGroupRegion(iSeqAnalysisGroup1: RDD[((Long, Int), Seq[Int])], iSeqAnalysisGroup2: RDD[((Long, Int), Seq[Int])]): RDD[((Long, Int), (Seq[Int], Seq[Int]))] = {
    /*  val sAnalysisG1 = iSeqAnalysisGroup1.partitionBy(new RangePartitioner[Long, Seq[Int]](72, iSeqAnalysisGroup1))
      val sAnalysisG2 = iSeqAnalysisGroup2.partitionBy(new RangePartitioner[Long, Seq[Int]](72, iSeqAnalysisGroup2))
      val seqJoint = sAnalysisG1.cogroup(sAnalysisG2)*/
    val seqJoint: RDD[((Long, Int), (Seq[Seq[Int]], Seq[Seq[Int]]))] = iSeqAnalysisGroup1.cogroup(iSeqAnalysisGroup2)
    val finalSeqJoint = seqJoint
      // .mapValues(r=>(r._1(0),r._2(0)))
      .map(r => (r._1,
      (if (r._2._1.length == 0) ArrayBuffer.fill[Int](caseSampleNum)(0) else r._2._1(0),
        if (r._2._2.length == 0) ArrayBuffer.fill[Int](controlSampleNum)(0) else r._2._2(0)))
      )
    return (finalSeqJoint)
  }

  private def computeTwoSampleCvMTest(iSeqCC: RDD[(Long, (Seq[Int], Seq[Int]))]): RDD[((Int, Double), (Long, Double, Double, Double))] = {
    /*((chrNum,p-value),(pos,FC,avgCountA,avgCountB)*/

    val twoSampleTests = iSeqCC
      .map(r => (r._1, r._2, SparkSeqCvM2STest.getTestStatistics(r._2._1, r._2._2)))
      .map(r => ((r._1), (r._2, r._3, SparkSeqCvM2STest.getPValue(r._3, cmDistTableB),
      SparkSeqStats.mean(r._2._1) / SparkSeqStats.mean(r._2._2), SparkSeqStats.mean(r._2._1), SparkSeqStats.mean(r._2._2))))
      .map(r => (((r._1 / 1000000000L).toInt, r._2._3), (r._1, r._2._4, r._2._5, r._2._6)))
      .filter(r => r._1._2 <= iMaxPval)
    return (twoSampleTests)
  }

  private def coalesceContRegions(iRegRDD: RDD[(Double, Int, (String, Int), Double, String, Int, Double, Double, Double, Int)]):
  RDD[(Double, Int, (String, Int), Double, String, Int, Double, Double, Double, Int)] = {
    val coalRegions = iRegRDD.map(r => (((if (r._6 == 0) "NEWREG" + r._3._1 else r._5), r._6, r._3._1), (r._1, r._2, r._3, r._4, r._7, r._8, r._9, r._10)))
      /*( (geneId,exonId,chrName), (pval,length,(chr,startPos), foldChange, pctOverlap,avgCountA,avgCountB) ) */
      .groupByKey()
      .mapValues(r => (r.sortBy(_._3._2)))
      .mapPartitions {
      var k = 0
      partitionIterator =>
        var regLenArray = new Array[(Double, Int, (String, Int), Double, String, Int, Double, Double, Double, Int)](1000000)
        for (r <- partitionIterator) {
          var regStart = r._2(0)._3._2
          var regLength = 0
          var fcWeightSum = 0.0
          var pctOverlapSum = 0.0
          var maxPval = 0.0
          var avgCountA = 0.0
          var avgCountB = 0.0
          var i = 0
          while (i < r._2.length) {
            if (i == (r._2.length - 1)) {
              if ((r._2.length == 1) || ((r._2(i)._3._2 - 1 != (r._2(i - 1)._3._2 + r._2(i - 1)._2) || (coalesceRegDiffPVal != true && (r._2(i)._1 != r._2(i - 1)._1)) ||
                (math.signum(r._2(i)._4) != math.signum(r._2(i - 1)._4))))) {
                regLength = r._2(i)._2
                if (regLength >= iMinRegionLen) {
                  //maxPval = if (maxPval < r._2(i)._1) r._2(i)._1 else maxPval
                  regLenArray(k) = (r._2(i)._1, regLength, (r._2(i)._3), r._2(i)._4, r._1._1, r._1._2,
                    math.round(r._2(i)._5 * 10000).toDouble / 10000, r._2(i)._6, r._2(i) _7, r._2(i)._8)
                  k += 1
                }
                maxPval = 0.0
              }
              else {
                fcWeightSum = (fcWeightSum * regLength + r._2(i)._4 * r._2(i)._2) / (regLength + r._2(i)._2).toDouble

                pctOverlapSum += r._2(i)._5
                avgCountA = (avgCountA * regLength + r._2(i)._6 * r._2(i)._2) / (regLength + r._2(i)._2)
                avgCountB = (avgCountB * regLength + r._2(i)._7 * r._2(i)._2) / (regLength + r._2(i)._2)
                regLength += r._2(i)._2
                maxPval = if (maxPval < r._2(i)._1) r._2(i)._1 else maxPval
                if (regLength >= iMinRegionLen) {
                  regLenArray(k) = (maxPval, regLength, (r._2(i)._3._1, regStart), fcWeightSum, r._1._1, r._1._2,
                    math.round(pctOverlapSum * 10000).toDouble / 10000, avgCountA, avgCountB, r._2(i)._8)
                  k += 1
                }
                maxPval = 0.0
              }


            }
            else if (r._2(i + 1)._3._2 - 1 != (r._2(i)._3._2 + r._2(i)._2) || (coalesceRegDiffPVal != true && (r._2(i)._1 != r._2(i - 1)._1)) ||
              (math.signum(r._2(i + 1)._4) != math.signum(r._2(i)._4))) {

              fcWeightSum = (fcWeightSum * regLength + r._2(i)._4 * r._2(i)._2) / (regLength + r._2(i)._2).toDouble
              avgCountA = (avgCountA * regLength + r._2(i)._6 * r._2(i)._2) / (regLength + r._2(i)._2)
              avgCountB = (avgCountB * regLength + r._2(i)._7 * r._2(i)._2) / (regLength + r._2(i)._2)
              regLength += r._2(i)._2
              pctOverlapSum += r._2(i)._5
              maxPval = if (maxPval < r._2(i)._1) r._2(i)._1 else maxPval

              if (regLength >= iMinRegionLen) {
                regLenArray(k) = (maxPval, regLength, (r._2(i)._3._1, regStart), fcWeightSum, r._1._1, r._1._2,
                  math.round(pctOverlapSum * 10000).toDouble / 10000, avgCountA, avgCountB, r._2(i)._8)
                k += 1
              }
              regLength = 0
              fcWeightSum = 0.0
              regStart = r._2(i + 1)._3._2
              pctOverlapSum = 0.0
              maxPval = 0.0
              avgCountA = 0.0
              avgCountB = 0.0
            }
            else {
              fcWeightSum = (fcWeightSum * regLength + r._2(i)._4 * r._2(i)._2) / (regLength + r._2(i)._2).toDouble
              avgCountA = (avgCountA * regLength + r._2(i)._6 * r._2(i)._2) / (regLength + r._2(i)._2)
              avgCountB = (avgCountB * regLength + r._2(i)._7 * r._2(i)._2) / (regLength + r._2(i)._2)
              regLength += r._2(i)._2
              pctOverlapSum += r._2(i)._5
              maxPval = if (maxPval < r._2(i)._1) r._2(i)._1 else maxPval

            }
            i += 1
          }
        }
        Iterator(regLenArray.filter(r => r != null).sortBy(-_._2))
    }.flatMap(r => r)
    return coalRegions
  }

  private def findContRegionsEqual(iSeq: RDD[((Int, Double), Seq[(Long, Double, Double, Double)])]):
  RDD[(Double, Int, (String, Int), Double, String, Int, Double, Double, Double, Int)] = {
    val iSeqPart = iSeq.map(r => (r._1._2, r._2.sortBy(_._1)))
    //.partitionBy(new HashPartitioner(iNumTasks * 3))
    iSeqPart
      .mapPartitions {
      partitionIterator =>
        var regLenArray = new Array[(Double, Int, (String, Int), Double, String, Int, Double, Double, Double, Int)](1000000)
        var k = 0
        for (r <- partitionIterator) {
          var regStart = r._2(0)._1
          var regLength = 1
          var fcSum = 0.0
          var i = 1
          var avgCountA = 0.0
          var avgCountB = 0.0
          while (i < r._2.length) {
            if (i == r._2.length - 1) {
              if (r._2(i)._2 - 1 != r._2(i - 1)._2) {
                fcSum += r._2(i - 1)._2
                avgCountA = (avgCountA * regLength + r._2(i - 1)._3) / (regLength + 1)
                avgCountB = (avgCountB * regLength + r._2(i - 1)._4) / (regLength + 1)
                for (r <- mapRegionsToExons((r._1, regLength, regStart, fcSum / regLength))) {
                  regLenArray(k) = (r._1, r._2, r._3, r._4, r._5, r._6, r._7, avgCountA, avgCountB, r._8)
                  k += 1
                }
                regLength = 1
                fcSum = r._2(i)._2
                avgCountA = r._2(i)._3
                avgCountB = r._2(i)._4
                for (r <- mapRegionsToExons((r._1, regLength, regStart, fcSum / regLength))) {
                  regLenArray(k) = (r._1, r._2, r._3, r._4, r._5, r._6, r._7, avgCountA, avgCountB, r._8)
                  k += 1
                }

              }
              else {
                fcSum += r._2(i)._2
                avgCountA = (avgCountA * regLength + r._2(i - 1)._3) / (regLength + 1)
                avgCountB = (avgCountB * regLength + r._2(i - 1)._4) / (regLength + 1)
                regLength += 1
                for (r <- mapRegionsToExons((r._1, regLength, regStart, fcSum / regLength))) {
                  regLenArray(k) = (r._1, r._2, r._3, r._4, r._5, r._6, r._7, avgCountA, avgCountB, r._8)
                  k += 1
                }

              }

            }
            else if (r._2(i)._1 - 1 != r._2(i - 1)._1) {
              // if (regLength >= iMinRegionLen) {
              fcSum += r._2(i - 1)._2
              avgCountA = (avgCountA * regLength + r._2(i - 1)._3) / (regLength + 1)
              avgCountB = (avgCountB * regLength + r._2(i - 1)._4) / (regLength + 1)
              for (r <- mapRegionsToExons((r._1, regLength, regStart, fcSum / regLength))) {
                regLenArray(k) = (r._1, r._2, r._3, r._4, r._5, r._6, r._7, avgCountA, avgCountB, r._8)
                k += 1
              }
              //}
              regLength = 1
              fcSum = 0.0
              regStart = r._2(i)._1
              avgCountA = 0.0
              avgCountB = 0.0

            }
            else {
              fcSum += (r._2(i - 1)._2)
              avgCountA = (avgCountA * regLength + r._2(i - 1)._3) / (regLength + 1)
              avgCountB = (avgCountB * regLength + r._2(i - 1)._4) / (regLength + 1)
              regLength += 1
            }
            i = i + 1
          }
        }
        Iterator(regLenArray.filter(r => r != null).sortBy(-_._2))
    }.flatMap(r => r)
  }

  private def findContRegionsLessEqual(iSeq: RDD[(Int, Seq[(Double, Long, Double, Double, Double)])]) /*(chrId,(pval,position,foldChange) */
  : RDD[(Double, Int, (String, Int), Double, String, Int, Double, Double, Double, Int)] = {
    val iSeqPart = iSeq.mapValues(r => (r.sortBy(_._2)))
    // .partitionBy(new HashPartitioner(iNumTasks * 3))
    iSeqPart
      .mapPartitions {
      partitionIterator =>
        var regLenArray = new Array[(Double, Int, (String, Int), Double, String, Int, Double, Double, Double, Int)](1000000)
        var k = 0
        for (r <- partitionIterator) {
          var regStart = r._2(0)._2
          var regLength = 1
          var fcSum = 0.0
          var i = 1
          var maxPval = 0.0
          var avgCountA = 0.0
          var avgCountB = 0.0
          while (i < r._2.length) {
            if (i == r._2.length - 1) {
              if (r._2(i)._2 - 1 != r._2(i - 1)._2) {
                maxPval = if (maxPval < r._2(i - 1)._1) r._2(i - 1)._1 else maxPval
                fcSum += r._2(i - 1)._3
                avgCountA = (avgCountA * regLength + r._2(i - 1)._4) / (regLength + 1)
                avgCountB = (avgCountB * regLength + r._2(i - 1)._5) / (regLength + 1)
                for (r <- mapRegionsToExons((maxPval, regLength, regStart, fcSum / regLength))) {
                  regLenArray(k) = (r._1, r._2, r._3, r._4, r._5, r._6, r._7, avgCountA, avgCountB, r._8)
                  k += 1
                }
                regLength = 1
                fcSum = r._2(i)._3
                maxPval = r._2(i)._1
                avgCountA = r._2(i)._4
                avgCountB = r._2(i)._5
                for (r <- mapRegionsToExons((maxPval, regLength, regStart, fcSum / regLength))) {
                  regLenArray(k) = (r._1, r._2, r._3, r._4, r._5, r._6, r._7, avgCountA, avgCountB, r._8)
                  k += 1
                }

            }
              else {
                maxPval = if (maxPval < r._2(i - 1)._1) r._2(i - 1)._1 else maxPval
              fcSum += r._2(i - 1)._3
                regLength += 1
                avgCountA = (avgCountA * regLength + r._2(i - 1)._4) / (regLength + 1)
                avgCountB = (avgCountB * regLength + r._2(i - 1)._5) / (regLength + 1)
                for (r <- mapRegionsToExons((maxPval, regLength, regStart, fcSum / regLength))) {
                  regLenArray(k) = (r._1, r._2, r._3, r._4, r._5, r._6, r._7, avgCountA, avgCountB, r._8)
                  k += 1
                }

              }

            }
            else if (r._2(i)._2 - 1 != r._2(i - 1)._2) {
              //if (regLength >= iMinRegionLen) {
              maxPval = if (maxPval < r._2(i - 1)._1) r._2(i - 1)._1 else maxPval
              fcSum += r._2(i - 1)._3
              avgCountA = (avgCountA * regLength + r._2(i - 1)._4) / (regLength + 1)
              avgCountB = (avgCountB * regLength + r._2(i - 1)._5) / (regLength + 1)
              for (r <- mapRegionsToExons((maxPval, regLength, regStart, fcSum / regLength))) {
                regLenArray(k) = (r._1, r._2, r._3, r._4, r._5, r._6, r._7, avgCountA, avgCountB, r._8)
                k += 1
              }
              //}
              regLength = 1
              fcSum = 0.0
              regStart = r._2(i)._2
              maxPval = 0.0
              avgCountA = 0.0
              avgCountB = 0.0

            }
            else {
              avgCountA = (avgCountA * regLength + r._2(i - 1)._4) / (regLength + 1)
              avgCountB = (avgCountB * regLength + r._2(i - 1)._5) / (regLength + 1)
              regLength += 1
              fcSum += r._2(i - 1)._3
              maxPval = if (maxPval < r._2(i - 1)._1) r._2(i - 1)._1 else maxPval
            }
            i = i + 1
          }
        }
        Iterator(regLenArray.filter(r => r != null).sortBy(-_._2))
    }.flatMap(r => r) /*(chrId,(pval,position,foldChange) */


  }

  private def getRangeIntersect(r1Start: Int, r1End: Int, r2Start: Int, r2End: Int): (Int, Int) = {
    val maxStart = math.max(r1Start, r2Start)
    val minEnd = math.min(r1End, r2End)
    (maxStart, minEnd)
  }

  private def getExonFromPosition(iChr: String, iStartPos: Int): (String, Int, Int, Int, Int) = {
    if (genExonsMapB.value.contains(iChr)) {
      val id = iStartPos / 10000
      val exons = genExonsMapB.value(iChr)
      var exonTuple = ("", 0, 0, 0, -1)
      val loop = new Breaks
      loop.breakable {
        if (exons(id) != null) {
          for (e <- exons(id)) {
            if (iStartPos >= e._3 && iStartPos <= e._4) {
              exonTuple = e
              loop.break
            }
          }
        }
      }
      return exonTuple
    }
    else
      return ("ExonNotFound", 0, 0, 0, -1)
  }

  private def getExonRange(iGeneId: String, iExonId: Int, tId: Int): (Int, Int) = {
    return genExonsMapLookupB.value((iGeneId, iExonId, tId))
  }

  private def mapRegionsToExons(r: (Double, Int, Long, Double)): ArrayBuffer[(Double, Int, (String, Int), Double, String, Int, Double, Int)] = {

    val reg = (r._1, r._2, SparkSeqConversions.idToCoordinates(r._3), r._4)
    val regionsArray = ArrayBuffer[(Double, Int, (String, Int), Double, String, Int, Double, Int)]()
    val exonsOverlapHashMap = scala.collection.mutable.HashMap[(String, Int), (Double, Int, Int, Int, Int)]() /*(genId,exId)(pctOverLap,overLapLength,tId,exStart,exEnd)*/
    if (genExonsMapB.value.contains(reg._3._1)) {
      val exons = genExonsMapB.value(reg._3._1)
      var exId = 0
      var genId = ""
      var tId = 0
      val id = reg._3._2 / 10000
      var exonOverlapPct = 0.0
      val loop = new Breaks
      loop.breakable {
        for (i <- math.max(0, id - 5) to math.min(id + 5, exons.length - 1)) {
          if (exons(i) != null) {
            for (e <- exons(i)) {
              val regStart = reg._3._2
              val regEnd = reg._3._2 + reg._2
              val exonIntersect = getRangeIntersect(regStart, regEnd, e._3, e._4)
              val exonIntersectLen = exonIntersect._2 - exonIntersect._1
              if (exonIntersectLen > 1 /*|| (exonIntersectLen == 1 && (exonIntersectLen.toDouble / (e._4 - e._3)) >= minExonPct)*/ ) {
                exonOverlapPct = exonIntersectLen.toDouble / (e._4 - e._3)
                exId = e._2
                genId = e._1
                tId = e._5
                var rangeInter = (0, 0)
                var maxIntersectLength = 0
                var maxOverlapPct = 0.0
                var maxId: (String, Int) = ("", 0)
                for (k <- exonsOverlapHashMap) {
                  rangeInter = getRangeIntersect(k._2._4, k._2._5, regStart, regEnd)
                  if (genId == k._1._1 && maxIntersectLength <= rangeInter._2 - rangeInter._1 && maxOverlapPct <= k._2._1) {
                    maxIntersectLength = rangeInter._2 - rangeInter._1
                    maxId = k._1
                    maxOverlapPct = k._2._1
                  }
                }
                if (maxIntersectLength < 1)
                  exonsOverlapHashMap((genId, exId)) = (exonOverlapPct, exonIntersectLen, tId, e._3, e._4)
                else if (maxOverlapPct <= exonOverlapPct) {
                  exonsOverlapHashMap.remove(maxId)
                  exonsOverlapHashMap((genId, exId)) = (exonOverlapPct, exonIntersectLen, tId, e._3, e._4)
                }
                //loop.break() //because there are some overlapping regions


              }

            }

          }
          else if (regionsArray.length == 0 && exonsOverlapHashMap.size == 0 && i == math.min(id + 5, exons.length - 1))
            regionsArray += ((reg._1, reg._2, reg._3, reg._4, "PositionNotFound", 0, 0.0, -1))
        }
        for (ex <- exonsOverlapHashMap)
          regionsArray += ((reg._1, reg._2, reg._3, reg._4, ex._1._1, ex._1._2, math.round(ex._2._1 * 10000).toDouble / 10000, ex._2._3))
      }

    }
    else
      regionsArray += ((reg._1, reg._2, reg._3, reg._4, "ChrNotFound", 0, 0.0, -1))

    return regionsArray
  }

  /**
   *
   * @param iCoalesceRegDiffPVal If continuous regions of different p-value <=iMaxPval should be coalesed (default false).
   * @return RDD of tuples(p-value,regionLength, (chrom,starPosition),foldChange,genId,exonId,exonRegionOverlap)
   */
  def findCandRegions(iCoalesceRegDiffPVal: Boolean = false): (scala.collection.mutable.HashMap[String, Array[ArrayBuffer[(String, Int, Int, Int, Int)]]],
    scala.collection.mutable.HashMap[String, Array[ArrayBuffer[(String, Int, Int, Int, Int)]]]) = {

    coalesceRegDiffPVal = iCoalesceRegDiffPVal
    val seqGroupCase = groupSeqAnalysis(iSeqAnalCase, caseSampleNum)
    //seqGroupCase.saveAsTextFile("hdfs://sparkseq002.cloudapp.net:9000/BAM/debugBaseCase")
    val seqGroupControl = groupSeqAnalysis(iSeqAnalControl, controlSampleNum)
    //seqGroupControl.saveAsTextFile("hdfs://sparkseq002.cloudapp.net:9000/BAM/debugBaseControl")
    val seqJointCC = joinSeqAnalysisGroup(seqGroupCase, seqGroupControl)
    //seqJointCC.saveAsTextFile("hdfs://sparkseq002.cloudapp.net:9000/BAM/debugBaseJoint")
    val seqFilterCC = seqJointCC.filter(r => (SparkSeqStats.mean(r._2._1) >= iMinCoverage || SparkSeqStats.mean(r._2._2) >= iMinCoverage))
    //seqFilterCC.saveAsTextFile("hdfs://sparkseq002.cloudapp.net:9000/BAM/debugBaseFilter")
    val seqCompTest = computeTwoSampleCvMTest(seqFilterCC)
    //seqCompTest.saveAsTextFile("hdfs://sparkseq002.cloudapp.net:9000/BAM/debugBaseTest")
    val seqPValGroup = seqCompTest
    if (iCoalesceRegDiffPVal == false)
      seqRegContDERDD = findContRegionsEqual(seqPValGroup.groupByKey())
    else {
      val seqPrePart = seqPValGroup
        .map(r => (r._1._1, (r._1._2, r._2._1, r._2._2, r._2._3, r._2._4)))
        .groupByKey()

      val seqPostPar = {
        seqPrePart.partitionBy(new RangePartitioner[Int, Seq[(Double, Long, Double, Double, Double)]](iNumTasks, seqPrePart))
      }
      seqRegContDERDD = findContRegionsLessEqual(seqPostPar)
      //seqRegContDERDD.saveAsTextFile("hdfs://sparkseq002.cloudapp.net:9000/BAM/debugRegCoals")
    }
    val seqReg = {
      seqRegContDERDD.map(r => (r._1, r._2, r._3, (if (r._4 < 1.0) (-1.0 / r._4) else r._4), r._5, r._6, r._7, r._8, r._9, r._10))
    }
    seqRegDERDDPhase1 = coalesceContRegions(seqReg)
    seqRegDERDDPhase1.saveAsTextFile("hdfs://sparkseq002.cloudapp.net:9000/BAM/debugRegColasCont")
    val newRegCandidates = getRegionCandidates()
    debugSaveCandidates(newRegCandidates, iFilePathLacal = "regions_candidates_" + minRegLen.toString + "_" + chrName + "_" + maxPval.toString + ".txt")

    val exonCandidates = getDistExonCandidates()
    debugSaveCandidates(exonCandidates, iFilePathLacal = "exons_candidates_" + minRegLen.toString + "_" + chrName + "_" + maxPval.toString + ".txt")
    return (exonCandidates, newRegCandidates)
  }


  def permutTestRegions(iRegions: broadcast.Broadcast[scala.collection.mutable.
  HashMap[String, Array[scala.collection.mutable.ArrayBuffer[(String, Int, Int, Int, Int)]]]]): RDD[((String, Int), Int, Double, Double)] = {
    val seqRegCovCase = iSeqAnalCase.getCoverageRegion(iRegions).map(r => ((SparkSeqConversions.stripSampleID(r._1._1), r._1._2), r._2))
      .groupByKey()
      .mapValues(c => if ((caseSampleNum - c.length) > 0) (c ++ ArrayBuffer.fill[Int](caseSampleNum - c.length)(0)) else (c))
    val seqRegCovControl = iSeqAnalControl.getCoverageRegion(iRegions).map(r => ((SparkSeqConversions.stripSampleID(r._1._1), r._1._2), r._2))
      .groupByKey()
      .mapValues(c => if ((controlSampleNum - c.length) > 0) (c ++ ArrayBuffer.fill[Int](controlSampleNum - c.length)(0)) else (c))
    val jointRegion = joinSeqAnalysisGroupRegion(seqRegCovCase, seqRegCovControl)
    val permTestRegion = jointRegion.map {
      r =>
        val statTests = Array[StatisticalTest](SparkSeqCvM2STest)
        val permTest = new AdaptivePermutTest(iNPermut = 10000, iStatTests = statTests, r._2._1, r._2._2)
        ((SparkSeqConversions.ensemblRegionIdToGenExonId(r._1._1)), r._1._2, permTest.getPvalue(), SparkSeqStats.mean(r._2._1) / SparkSeqStats.mean(r._2._2))
    }

    // .filter(r=>r._2<=iMaxPval)
    seqRegDERDDExons = seqRegDERDDPhase1
      .filter(r => r._3._2 != 0)
      .map(r => (r._10, (r._1, r._2, r._3, r._4, r._5, r._6, r._7, r._8, r._9))).join(permTestRegion.map(r => (r._2, r._3)))
      .mapValues(r => (r._1._1, r._1._2, r._1._3, r._1._4, r._1._5, r._1._6, r._1._7, r._1._8, r._1._9, r._2))
    return permTestRegion
  }

  private def fetchReultsExons(num: Int): Array[(Int, (Double, Int, (String, Int), Double, String, Int, Double, Double, Double, Double))] = {
    val results = seqRegDERDDExons.coalesce(1).takeOrdered(num)(Ordering[(Double, Double, Int)]
      .on(r => (r._2._10, -(math.abs(r._2._4)), -r._2._2)))
    Thread.sleep(100)
    return (results)
  }


  /**
   *
   * @param iNum Number of top regions sorted  by p-value asc, foldChange desc and region length desc to be printed (default 10000).
   */
  def printResults(iNum: Int = 10000) = {

    val a = fetchReultsExons(iNum)
    val header = "p-value".toString.padTo(10, ' ') + "foldChange".padTo(25, ' ') + "length".padTo(10, ' ') +
      "Coordinates".padTo(20, ' ') + "geneId".padTo(25, ' ') + "exonId".padTo(10, ' ') + "exonOverlapPct"
    println("=======================================Results======================================")
    println(header)

    for (r <- a) {
      val rec = (math.round(r._1 * 100000).toDouble / 100000).toString.padTo(10, ' ') + (math.round(r._2._4 * 10000).toDouble / 10000).toString.padTo(25, ' ') +
        r._2.toString.padTo(10, ' ') + r._2._3.toString.padTo(20, ' ') + r._2._5.toString.padTo(25, ' ') + r._2._6.toString.padTo(10, ' ') + r._2._7
      println(rec)
    }

  }

  /**
   *
   * @param iNum Number of top regions sorted  by p-value asc, foldChange desc and region length desc to be saved to file (default 10000).
   * @param iFilePathLocal Local path to save top iNum regions locally.
   * @param iFilePathRemote Remote path to HDFS storage to save all the results.
   */
  def saveResults(iNum: Int = 10000, iFilePathLocal: String = "sparkseq_10000.txt", iFilePathRemote: String) = {
    if (iNum <= 10000) {
      val localResults = fetchReultsExons(iNum)
      val writer = new PrintWriter(new File(iFilePathLocal))
      val header = "Ph-II p-val".toString.padTo(15, ' ') + "Ph-I p-val".toString.padTo(15, ' ') + "foldChange".padTo(25, ' ') + "length".padTo(10, ' ') +
        "Coordinates".padTo(20, ' ') + "geneId".padTo(25, ' ') + "exonId".padTo(10, ' ') + "exonOverlapPct".padTo(15, ' ') +
        "avgCovA".padTo(10, ' ') + "avgCovB".padTo(10, ' ') + "covSignifficant".padTo(20, ' ') + "genExonTranId".padTo(10, ' ')
      //println("=======================================Results======================================"B
      writer.write(header + "\n")
      for (r <- localResults) {
        var rec = (math.round(r._2._10 * 100000).toDouble / 100000).toString.padTo(15, ' ') + (math.round(r._2._1 * 100000).toDouble / 100000).toString.padTo(15, ' ') +
          (math.round(r._2._4 * 100000).toDouble / 100000).toString.padTo(25, ' ') +
          r._2._2.toString.padTo(10, ' ') + r._2._3.toString.padTo(20, ' ') + r._2._5.toString.padTo(25, ' ') + r._2._6.toString.padTo(10, ' ') + r._2._7.toString.padTo(15, ' ') +
          ((math.round(r._2._8 * 100)).toDouble / 100).toString.padTo(10, ' ') + ((math.round(r._2._9 * 100) / 100).toDouble).toString.padTo(10, ' ') +
          (if (r._2._8 < 2 && r._2._9 < 2) "*" else if (r._2._8 >= 100 || r._2._9 >= 100) "****" else if (r._2._8 >= 10 || r._2._9 >= 10) "***" else "**").padTo(20, ' ') +
          r._1.toString.padTo(10, ' ')
        writer.write(rec + "\n")
      }
      writer.close()
    }
    seqRegDERDDExons.saveAsTextFile(iFilePathRemote)
  }

  private def debugSaveCandidates(iCandMap: scala.collection.mutable.HashMap[String, Array[ArrayBuffer[(String, Int, Int, Int, Int)]]],
                                  iFilePathLacal: String = "sparkseq_candidates.txt") = {
    val writer = new PrintWriter(new File(iFilePathLacal))
    for (r <- iCandMap) {
      for (r1 <- r._2) {
        if (r1 != null)
          for (r3 <- r1)
            writer.write(r._1 + "," + r3.toString() + "\n")
      }
    }
    writer.close()
  }

  def getDistExonCandidates(): scala.collection.mutable.HashMap[String, Array[ArrayBuffer[(String, Int, Int, Int, Int) /*(GeneId,ExonId,Start,End,tId)*/ ]]] = {

    val exonCand = seqRegDERDDPhase1 /*genExons format: (genId,ExonId,chr,start,end,strand)*/
      .filter(r => (r._6 > 0)) //filter out uknown regions
      .map {
      r => val eRange = getExonRange(r._5, r._6, r._10); (r._5, r._6, r._3._1, eRange._1, eRange._2, ".", r._10)
    }.distinct.collect()
    val exonCandHashMap = SparkSeqConversions.exonsToHashMap(exonCand)
    return (exonCandHashMap)
  }


  def getRegionCandidates(): scala.collection.mutable.HashMap[String, Array[ArrayBuffer[(String, Int, Int, Int, Int) /*(GeneId,ExonId,Start,End)*/ ]]] = {
    val unRegionCand = seqRegDERDDPhase1 /*genExons format: (genId,ExonId,chr,start,end,strand)*/
      .filter(r => (r._6 == 0))
      .map(r => ("", 0, r._3._1, r._3._2, r._3._2 + r._2, ".", -1)).distinct().collect()
    val newRegPreffix = "NEWREG"
    val nameLength = 15
    for (k <- 0 to unRegionCand.length - 1) {
      val newRegId = newRegPreffix.padTo(nameLength - (k + 1).toString.length, '0') + (k + 1).toString
      val t = unRegionCand(k)
      unRegionCand(k) = (newRegId, t._2, t._3, t._4, t._5, t._6, t._7)
    }

    val unRegionCandHashMap = SparkSeqConversions.exonsToHashMap(unRegionCand)


    return (unRegionCandHashMap)

  }
}
