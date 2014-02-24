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

/**
 * Created by mwiewior on 2/24/14.
 */
class SparkSeqDiffExpr(iSC: SparkContext, iSeqAnalCase: SparkSeqAnalysis, iSeqAnalControl: SparkSeqAnalysis, iChr: String,
                       iStartPos: Int = 1, iEndPos: Int = 300000000, iMinCoverage: Int = 10, iMinRegionLen: Int = 1,
                       iMaxPval: Double = 0.1, iReduceWorkers: Int = 8, confDir: String) extends Serializable {

  private val caseSampleNum: Int = iSeqAnalCase.bamFile.count.toInt
  private val controlSampleNum: Int = iSeqAnalControl.bamFile.count.toInt
  var diffExprRDD: RDD[(Double, Double, Int, (String, Int), String, Int, Double)] = new EmptyRDD[(Double, Double, Int, (String, Int), String, Int, Double)](iSC)

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
    val finalSeqJoint = leftSeqJoint.map(r => (r._1, Option(r._2._1), r._2._2)).union(rightSeqJoint.map(r => (r._1, r._2._1, Option(r._2._2)))).map(r => (r._1, (r._2, r._3)))
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

  private def computeTwoSampleCvMTest(iSeqCC: RDD[(Long, (Seq[Int], Seq[Int]))]): RDD[(Double, (Long, Int))] = {
    val cmDistTable = iSC.textFile(confDir + "cm" + caseSampleNum + "_" + controlSampleNum + "_2.txt")
      .map(l => l.split("\t"))
      .map(r => (r.array(0).toDouble, r.array(1).toDouble))
      .toArray
    val cmDistTableB = iSC.broadcast(cmDistTable)
    val twoSampleTests = iSeqCC
      .map(r => (r._1, r._2, SparkSeqCvM2STest.computeTestStat(r._2._1, r._2._2)))
      .map(r => ((r._1), (r._2, r._3, SparkSeqCvM2STest.getPValue(r._3, cmDistTableB), SparkSeqStats.mean(r._2._1) / SparkSeqStats.mean(r._2._2))))
      .map(r => (r._2._3, (r._1, r._2._4))) //pick position and p-value

  }

  def computeDiffExpr(): RDD[(Double, Double, Int, (String, Int), String, Int, Double)] = {

    val seqGroupCase = groupSeqAnalysis(iSeqAnalCase, caseSampleNum)
    val seqGroupControl = groupSeqAnalysis(iSeqAnalControl, controlSampleNum)
    val seqJointCC = joinSeqAnalysisGroup(seqGroupCase, seqGroupControl)
    val seqFilterCC = seqJointCC.filter(r => (SparkSeqStats.mean(r._2._1) > iMinCoverage || SparkSeqStats.mean(r._2._2) > iMinCoverage))
    val seqcompTest = computeTwoSampleCvMTest(seqFilterCC)

  }

  //def


}
