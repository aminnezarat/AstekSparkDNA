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

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import pl.elka.pw.sparkseq.seqAnalysis.SparkSeqAnalysis
import org.apache.spark.rdd._
import scala.collection.mutable.ArrayBuffer
import pl.elka.pw.sparkseq.statisticalTests._
import org.apache.spark.RangePartitioner
import pl.elka.pw.sparkseq.conversions.SparkSeqConversions
import scala.util.control._
import com.github.nscala_time.time.Imports._
import java.io._

/**
 * Created by mwiewior on 2/24/14.
 */
class SparkSeqDiffExpr(iSC: SparkContext, iSeqAnalCase: SparkSeqAnalysis, iSeqAnalControl: SparkSeqAnalysis, iBEDFile: String, iChr: String = "*",
                       iStartPos: Int = 1, iEndPos: Int = 300000000, iMinCoverage: Int = 10, iMinRegionLen: Int = 1,
                       iMaxPval: Double = 0.1, iNumTasks: Int = 8, iNumReducers: Int = 8, confDir: String) extends Serializable {

  private val caseSampleNum: Int = iSeqAnalCase.bamFile.count.toInt
  private val controlSampleNum: Int = iSeqAnalControl.bamFile.count.toInt
  var diffExprRDD: RDD[(Double, Int, (String, Int), Double, String, Int, Double)] = new EmptyRDD[(Double, Int, (String, Int), Double, String, Int, Double)](iSC)

  private def groupSeqAnalysis(iSeqAnalysis: SparkSeqAnalysis, iSampleNum: Int): RDD[(Long, Seq[Int])] = {
    val seqGrouped = iSeqAnalysis.getCoverageBaseRegion(iChr, iStartPos, iEndPos)
      .map(r => (r._1 % 1000000000000L, r._2))
      .groupByKey()
      .mapValues(c => if ((iSampleNum - c.length) > 0) (c ++ ArrayBuffer.fill[Int](iSampleNum - c.length)(0)) else (c))
    return (seqGrouped)
  }

  private def joinSeqAnalysisGroup(iSeqAnalysisGroup1: RDD[(Long, Seq[Int])], iSeqAnalysisGroup2: RDD[(Long, Seq[Int])]): RDD[(Long, (Seq[Int], Seq[Int]))] = {
    val leftSeqJoint = iSeqAnalysisGroup1.leftOuterJoin(iSeqAnalysisGroup2)
    val rightSeqJoint = iSeqAnalysisGroup1.rightOuterJoin(iSeqAnalysisGroup2)
    val finalSeqJoint = leftSeqJoint.map(r => (r._1, Option(r._2._1), r._2._2)).union(rightSeqJoint
      .map(r => (r._1, r._2._1, Option(r._2._2)))).map(r => (r._1, (r._2, r._3)))
      .map(r => (r._1, (r._2._1 match {
      case Some(x) => x;
      case None => ArrayBuffer.fill[Int](caseSampleNum)(0)
    },
      r._2._2 match {
        case Some(x) => x;
        case None => ArrayBuffer.fill[Int](controlSampleNum)(0)
      }))).distinct()
    return (finalSeqJoint)
  }

  private def computeTwoSampleCvMTest(iSeqCC: RDD[(Long, (Seq[Int], Seq[Int]))]): RDD[(Double, (Long, Double))] = {
    val cmDistTable = iSC.textFile(confDir + "cm" + caseSampleNum + "_" + controlSampleNum + "_2.txt")
      .map(l => l.split("\t"))
      .map(r => (r.array(0).toDouble, r.array(1).toDouble))
      .toArray
    val cmDistTableB = iSC.broadcast(cmDistTable)
    val twoSampleTests = iSeqCC
      .map(r => (r._1, r._2, SparkSeqCvM2STest.computeTestStat(r._2._1, r._2._2)))
      .map(r => ((r._1), (r._2, r._3, SparkSeqCvM2STest.getPValue(r._3, cmDistTableB), SparkSeqStats.mean(r._2._1) / SparkSeqStats.mean(r._2._2))))
      .map(r => (r._2._3, (r._1, r._2._4))) //pick position and p-value
    return (twoSampleTests)
  }

  private def findContRegionsEqual(iSeqPart: RDD[(Double, Seq[(Long, Double)])]): RDD[(Double, Int, Long, Double)] = {

    iSeqPart.map(r => (r._1, r._2.sortBy(_._1).distinct)).map(r => (r._1, r._2.distinct)) //2*x distinct workaround
      .mapPartitions {
      partitionIterator =>
        var regLenArray: ArrayBuffer[(Double, Int, Long, Double)] = ArrayBuffer()
        for (r <- partitionIterator) {
          var regStart = r._2(0)._1
          var regLength = 1
          var fcSum = 0.0
          var i = 1
          while (i < r._2.length) {
            if (r._2(i)._1 - 1 != r._2(i - 1)._1) {
              if (regLength >= iMinRegionLen)
                regLenArray += ((r._1, regLength, regStart, fcSum / regLength))
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
        Iterator(regLenArray.sortBy(-_._2))
    }.flatMap(r => r)
  }

  private def findContRegionsLessEqual() = {}

  private def mapRegionsToExons(iSeqReg: RDD[(Double, Int, Long, Double)]) = {
    val genExonsMapB = iSC.broadcast(SparkSeqConversions.BEDFileToHashMap(iSC, confDir + iBEDFile))
    iSeqReg.map(r => (r._1, r._2, SparkSeqConversions.idToCoordinates(r._3), r._4))
      .map(r =>
      if (genExonsMapB.value.contains(r._3._1)) {
        val exons = genExonsMapB.value(r._3._1)
        var exId = 0
        var genId = ""
        val id = r._3._2 / 10000
        var exonOverlapPct = 0.0
        val loop = new Breaks
        loop.breakable {
          if (exons(id) != null) {
            for (e <- exons(id)) {
              val exonIntersect = Range(r._3._2, r._3._2 + r._2).intersect(Range(e._3, e._4))
              if (exonIntersect.length > 0) {
                exonOverlapPct = (exonIntersect.max - exonIntersect.min).toDouble / (e._4 - e._3)
                exId = e._2
                genId = e._1
                //loop.break() //because there are some overlapping regions
              }
            }

          }
        }
        (r._1, r._2, r._3, r._4, genId, exId, math.round(exonOverlapPct * 10000).toDouble / 10000)
      }
      else
        (r._1, r._2, r._3, r._4, "ChrNotFound", 0, 0.0)
      )
  }

  def computeDiffExpr(): RDD[(Double, Int, (String, Int), Double, String, Int, Double)] = {

    val seqGroupCase = groupSeqAnalysis(iSeqAnalCase, caseSampleNum)
    val seqGroupControl = groupSeqAnalysis(iSeqAnalControl, controlSampleNum)
    val seqJointCC = joinSeqAnalysisGroup(seqGroupCase, seqGroupControl)
    val seqFilterCC = seqJointCC.filter(r => (SparkSeqStats.mean(r._2._1) > iMinCoverage || SparkSeqStats.mean(r._2._2) > iMinCoverage))
    val seqCompTest = computeTwoSampleCvMTest(seqFilterCC)
      .filter(r => r._1 <= iMaxPval)
    val seqPValGroup = seqCompTest.groupByKey(iNumTasks)

    val seqPValPartition = seqPValGroup
      .partitionBy(new RangePartitioner[Double, Seq[(Long, Double)]](iNumTasks, seqPValGroup))
    val seqReg = findContRegionsEqual(seqPValPartition)
    val seqRegExon = mapRegionsToExons(seqReg)

    diffExprRDD = seqRegExon
    return (seqRegExon)
  }

  private def fetchReults(): Array[(Double, Int, (String, Int), Double, String, Int, Double)] = {
    val results = diffExprRDD.toArray()
      .map(r => (r._1, r._2, r._3, if (r._4 < 1.0) -1 / r._4; else r._4, r._5, r._6, r._7))
      .sortBy(r => (r._1, -(math.abs(r._4)), -r._2))
    //    val b = finalcovJoint.take(10)
    //   b.foreach(println)
    iSC.stop()
    Thread.sleep(100)
    return (results)
  }

  def printResults = {

    val a = fetchReults()
    val header = "p-value".toString.padTo(10, ' ') + "foldChange".padTo(15, ' ') + "length".padTo(10, ' ') +
      "Coordinates".padTo(20, ' ') + "geneId".padTo(25, ' ') + "exonId".padTo(10, ' ') + "exonOverlapPct"
    println("=======================================Results======================================")
    println(header)

    for (r <- a) {
      val rec = r._1.toString.padTo(10, ' ') + (math.round(r._4 * 10000).toDouble / 10000).toString.padTo(15, ' ') +
        r._2.toString.padTo(10, ' ') + r._3.toString.padTo(20, ' ') + r._5.toString.padTo(25, ' ') + r._6.toString.padTo(10, ' ') + r._7
      println(rec)
    }

  }

  def saveResults(iFilePath: String) = {
    val a = fetchReults()
    val writer = new PrintWriter(new File(iFilePath))
    val header = "p-value".toString.padTo(10, ' ') + "foldChange".padTo(15, ' ') + "length".padTo(10, ' ') + "Coordinates".padTo(20, ' ') + "geneId".padTo(25, ' ') + "exonId".padTo(10, ' ') + "exonOverlapPct"
    println("=======================================Results======================================")
    writer.write(header + "\n")
    for (r <- a) {
      var rec = r._1.toString.padTo(10, ' ') + (math.round(r._4 * 10000).toDouble / 10000).toString.padTo(15, ' ') + r._2.toString.padTo(10, ' ') + r._3.toString.padTo(20, ' ') + r._5.toString.padTo(25, ' ') + r._6.toString.padTo(10, ' ') + r._7
      println(rec)
      writer.write(rec + "\n")
    }
    writer.close()

  }

}
