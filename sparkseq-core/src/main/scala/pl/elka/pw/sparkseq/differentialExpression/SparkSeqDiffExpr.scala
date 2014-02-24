package pl.elka.pw.sparkseq.differentialExpression

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import pl.elka.pw.sparkseq.seqAnalysis.SparkSeqAnalysis
import org.apache.spark.rdd._
import scala.collection.mutable.ArrayBuffer

/**
 * Created by mesos on 2/24/14.
 */
class SparkSeqDiffExpr(iSC: SparkContext, iSeqAnalCase: SparkSeqAnalysis, iSeqAnalControl: SparkSeqAnalysis, iChr: String,
                       iStartPos: Int = 1, iEndPos: Int = 30000000, iMinCoverage: Int = 10, iMinRegionLen: Int = 1, iMaxPval: Double = 0.1, iReduceWorkers: Int = 8) extends Serializable {

  val caseSampleNum: Int = iSeqAnalCase.bamFile.count.toInt
  val controlSampleNum: Int = iSeqAnalControl.bamFile.count.toInt

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


}
