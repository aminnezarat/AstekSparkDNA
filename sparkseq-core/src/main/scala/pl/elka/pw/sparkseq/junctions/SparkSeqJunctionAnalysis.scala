/**
 * Copyright (c) 2014. Marek Wiewiorka
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

package pl.elka.pw.sparkseq.junctions

import htsjdk.samtools.CigarOperator
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import pl.elka.pw.sparkseq.conversions.SparkSeqConversions
import pl.elka.pw.sparkseq.seqAnalysis.SparkSeqAnalysis

import scala.collection.mutable.ArrayBuffer

/**
 * Created by mwiewiorka on 4/4/14.
 */

/**
 * Class for junction reads analysis
 * @param seqAnalysis
 */
class SparkSeqJunctionAnalysis(seqAnalysis: SparkSeqAnalysis) extends Serializable {

  private val jSeqAnalysis = seqAnalysis
  private var junctionReads: RDD[((Long, Long), Int)] = _


  /**
   * Find reads with specified maximum number of gaps
   * @param maxGapNum maixmum number of gaps to look for
   */
  private def findGapReads(maxGapNum: Int = 3) = {

    //reads with 1 to maxGapNum gaps
    val cigRegex = ("^([0-9]+[MEQISXDHP]+)+([0-9]+N[0-9]+M[0-9]*[EQISXDHP]*){1," + maxGapNum.toString + "}[0-9]*[MEQISXDHP]*$").r
    jSeqAnalysis.filterCigarString(cigRegex.pattern.matcher(_).matches)

  }

  private def getGapFromCigar(alignStart: Int, cigar: htsjdk.samtools.Cigar): Array[Range] = {

    var gapArray = ArrayBuffer[Range]()
    val numCigElem = cigar.numCigarElements()
    val cigElements = cigar.getCigarElements()
    val cigIter = cigElements.listIterator()
    while (cigIter.hasNext) {
      val cigElem = cigIter.next()
      var shift = 0
      if (cigElem.getOperator != CigarOperator.N)
        shift += cigElem.getLength
      else {
        val start = alignStart + shift
        gapArray += Range(start, start + cigElem.getLength + 1)
        shift += cigElem.getLength
      }
    }
    return gapArray.toArray
  }

  /**
   * Get junctions counts across all samples in SparkSeqAnalysis object
   * @return ( (startPosID, endPosID), count )
   */
  def getJuncReadsCounts(maxGapNum: Int = 3): RDD[((Long, Long), Int)] = {

    //var junctions =  new EmptyRDD[((Long,Long),Int)]()
    findGapReads(maxGapNum=maxGapNum)
     val juncReads = jSeqAnalysis.getReads()
       .map(r => ((r._1, SparkSeqConversions.standardizeChr(r._2.getReferenceName), r._2.getAlignmentStart, r._2.getCigar), 1))
       .map {
       r =>
         val gapObjectsArray = new Array[((Long, Long), Int)](maxGapNum)
         val readCigar = r._1._4
         val alignStart = r._1._3
         val sampleID = r._1._1
         val chrName = r._1._2
         val gapArray = getGapFromCigar(alignStart, readCigar)
         var i = 0
         for (g <- gapArray) {
           gapObjectsArray(i) = ((SparkSeqConversions.getEncodedPosition(sampleID, chrName, g.start), SparkSeqConversions.getEncodedPosition(sampleID, chrName, g.last)), 1)
           i += 1
         }
         Iterator(gapObjectsArray.filter(r => r != null))
     }.flatMap(r => r).flatMap(r => r).reduceByKey(_ + _)

    junctionReads = juncReads
    return juncReads
  }

  /**
   * Print topN junction reads with the highest counts across samples
   * @param topN number of top junction reads to report
   */
  def viewJuncReadsCounts(topN: Int = 10) = {
    val juncReadsOrdered = junctionReads.coalesce(1).cache.takeOrdered(topN)(Ordering[(Int)]
      .on(r => -r._2))
    val header = "SampleID".padTo(10, ' ') + "ChrName".padTo(10, ' ') + "StartPos".padTo(20, ' ') + "EndPos".padTo(20, ' ') + "Count".padTo(10, ' ')
    println(header)
    println("".padTo(header.length, '='))
    for (j <- juncReadsOrdered) {
      val startPos = SparkSeqConversions.idToCoordinates(SparkSeqConversions.stripSampleID(j._1._1))
      val endPos = SparkSeqConversions.idToCoordinates(SparkSeqConversions.stripSampleID(j._1._2))
      println(SparkSeqConversions.splitSampleID(j._1._1)._1.toString.padTo(10, ' ') + startPos._1.padTo(10, ' ') + startPos._2.toString.padTo(20, ' ') +
        endPos._2.toString.padTo(20, ' ') + j._2.toString)

    }
  }
}
