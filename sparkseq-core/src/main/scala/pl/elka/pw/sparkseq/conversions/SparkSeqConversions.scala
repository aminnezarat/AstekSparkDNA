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
package pl.elka.pw.sparkseq.conversions

import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD

/**
 * Object for doing various data conversions used by SparkSeq.
 */
object SparkSeqConversions {

  /**
   * Method for converting string chromosome name to Long representation. It is used for
   * performance reasons.
   *
   * @param iRefName Chromosome name (e.g. chr1).
   * @return Chromosome name encoded as Long number.
   */
  def chrToLong(iRefName: String): Long = {

    val id = iRefName match {
      case "chr1" => 1000000000L
      case "chr2" => 2000000000L
      case "chr3" => 3000000000L
      case "chr4" => 4000000000L
      case "chr5" => 5000000000L
      case "chr6" => 6000000000L
      case "chr7" => 7000000000L
      case "chr8" => 8000000000L
      case "chr9" => 9000000000L
      case "chr10" => 10000000000L
      case "chr11" => 11000000000L
      case "chr12" => 12000000000L
      case "chr13" => 13000000000L
      case "chr14" => 14000000000L
      case "chr15" => 15000000000L
      case "chr16" => 16000000000L
      case "chr17" => 17000000000L
      case "chr18" => 18000000000L
      case "chr19" => 19000000000L
      case "chr20" => 20000000000L
      case "chr21" => 21000000000L
      case "chr22" => 22000000000L
      case "chr23" => 23000000000L
      case "chr24" => 24000000000L
      case "chr25" => 25000000000L
      case "chr26" => 26000000000L
      case "chr27" => 27000000000L
      case "chr28" => 28000000000L
      case "chr29" => 29000000000L
      case "chr30" => 30000000000L
      case "chr31" => 31000000000L
      case "chr32" => 32000000000L
      case "chr33" => 33000000000L
      case "chrX" => 100000000000L
      case "X" => 100000000000L
      case "chrY" => 110000000000L
      case "Y" => 110000000000L
      case "chrMT" => 120000000000L
      case _ => 900000000000L

    }
    return id

  }

  /**
   * Method for converting SparSeq internal gen position representation to tuple (chromosome, position)
   * @param id SparSeq internal gen position representation as Long number
   * @return Tuple (chromosome, position)
   */
  def idToCoordinates(id: Long): (String, Int) = {

    var coord = id match {
      case x if (x >= 1000000000L && x < 2000000000L) => ("chr1", (x - 1000000000L).toInt)
      case x if (x >= 2000000000L && x < 3000000000L) => ("chr2", (x - 2000000000L).toInt)
      case x if (x >= 3000000000L && x < 4000000000L) => ("chr3", (x - 3000000000L).toInt)
      case x if (x >= 4000000000L && x < 5000000000L) => ("chr4", (x - 4000000000L).toInt)
      case x if (x >= 5000000000L && x < 6000000000L) => ("chr5", (x - 5000000000L).toInt)
      case x if (x >= 6000000000L && x < 7000000000L) => ("chr6", (x - 6000000000L).toInt)
      case x if (x >= 7000000000L && x < 8000000000L) => ("chr7", (x - 7000000000L).toInt)
      case x if (x >= 8000000000L && x < 9000000000L) => ("chr8", (x - 8000000000L).toInt)
      case x if (x >= 9000000000L && x < 10000000000L) => ("chr9", (x - 9000000000L).toInt)
      case x if (x >= 10000000000L && x < 11000000000L) => ("chr10", (x - 10000000000L).toInt)
      case x if (x >= 11000000000L && x < 12000000000L) => ("chr11", (x - 11000000000L).toInt)
      case x if (x >= 12000000000L && x < 13000000000L) => ("chr12", (x - 12000000000L).toInt)
      case x if (x >= 13000000000L && x < 14000000000L) => ("chr13", (x - 13000000000L).toInt)
      case x if (x >= 14000000000L && x < 15000000000L) => ("chr14", (x - 14000000000L).toInt)
      case x if (x >= 15000000000L && x < 16000000000L) => ("chr15", (x - 15000000000L).toInt)
      case x if (x >= 16000000000L && x < 17000000000L) => ("chr16", (x - 16000000000L).toInt)
      case x if (x >= 17000000000L && x < 18000000000L) => ("chr17", (x - 17000000000L).toInt)
      case x if (x >= 18000000000L && x < 19000000000L) => ("chr18", (x - 18000000000L).toInt)
      case x if (x >= 19000000000L && x < 20000000000L) => ("chr19", (x - 19000000000L).toInt)
      case x if (x >= 20000000000L && x < 21000000000L) => ("chr20", (x - 20000000000L).toInt)
      case x if (x >= 21000000000L && x < 22000000000L) => ("chr21", (x - 21000000000L).toInt)
      case x if (x >= 22000000000L && x < 23000000000L) => ("chr22", (x - 22000000000L).toInt)
      case x if (x >= 23000000000L && x < 24000000000L) => ("chr23", (x - 23000000000L).toInt)
      case x if (x >= 24000000000L && x < 25000000000L) => ("chr24", (x - 24000000000L).toInt)
      case x if (x >= 2500000000L && x < 26000000000L) => ("chr25", (x - 25000000000L).toInt)
      case x if (x >= 2600000000L && x < 27000000000L) => ("chr26", (x - 26000000000L).toInt)
      case x if (x >= 2700000000L && x < 28000000000L) => ("chr27", (x - 27000000000L).toInt)
      case x if (x >= 2800000000L && x < 29000000000L) => ("chr28", (x - 28000000000L).toInt)
      case x if (x >= 2900000000L && x < 30000000000L) => ("chr29", (x - 29000000000L).toInt)
      case x if (x >= 3000000000L && x < 31000000000L) => ("chr30", (x - 30000000000L).toInt)
      case x if (x >= 3100000000L && x < 32000000000L) => ("chr31", (x - 31000000000L).toInt)
      case x if (x >= 3200000000L && x < 33000000000L) => ("chr32", (x - 32000000000L).toInt)
      case x if (x >= 3300000000L && x < 34000000000L) => ("chr33", (x - 33000000000L).toInt)
      case x if (x >= 100000000000L && x < 101000000000L) => ("chrX", (x - 100000000000L).toInt)
      case x if (x >= 110000000000L && x < 111000000000L) => ("chrY", (x - 110000000000L).toInt)
      case x if (x >= 120000000000L && x < 121000000000L) => ("chrMT", (x - 120000000000L).toInt)
      case x if (x >= 900000000000L && x < 901000000000L) => ("NA", (x - 900000000000L).toInt)
    }
    return coord
  }

  /**
   * Method for converting BED file to SparSeq internal HashMap
   * @param sc Apache Spark context.
   * @param bedFile Path to BED file.
   * @return SparkSeq internal representation of a BED as a HashMap
   */
  def BEDFileToHashMap(sc: SparkContext, bedFile: String): scala.collection.mutable.HashMap[String, Array[ArrayBuffer[(String, Int, Int, Int, Int) /*(GeneId,ExonId,Start,End)*/ ]]] = {

    val genExons = readBEDFile(sc, bedFile)
    /*genExons format: (genId,ExonId,chr,start,end,strand)*/
    val genExonsMap = exonsToHashMap(genExons)
    return genExonsMap
  }

  def BEDFileToHashMapGeneExon(sc: SparkContext, bedFile: String): scala.collection.mutable.HashMap[(String, Int, Int), (Int, Int)] = {
    val genExons = readBEDFile(sc, bedFile)
    var genExonsMap = new scala.collection.mutable.HashMap[(String, Int, Int), (Int, Int)]()
    for (r <- genExons)
      genExonsMap((r._1, r._2, r._7)) = (r._4, r._5)
    return genExonsMap
  }


  private def readBEDFile(iSC: SparkContext, iBedFile: String): Array[(String, Int, String, Int, Int, String, Int)] = {
    val genExons = iSC.textFile(iBedFile)
      .map(l => l.split("\t"))
      .map(r => (r.array(4).trim, r.array(5).trim.toInt, r.array(0).trim, r.array(1).trim.toInt, r.array(2).trim.toInt,
      r.array(3).trim, if (r.array.length >= 7 && r.array(6).trim != "") r.array(6).trim.toInt else -1)).toArray
    return genExons
  }

  /**
   *
   * @param iExons
   * @return
   */
  def exonsToHashMap(iExons: Array[(String, Int, String, Int, Int, String, Int)]): scala.collection.mutable.HashMap[String, Array[ArrayBuffer[(String, Int, Int, Int, Int)]]] = {

    val genExons = iExons
    var genExonsMap = scala.collection.mutable.HashMap[String, Array[ArrayBuffer[(String, Int, Int, Int, Int) /*(GeneId,ExonId,Start,End,tId)*/ ]]]()
    for (ge <- genExons) {
      if (!genExonsMap.contains(ge._3))
        genExonsMap(ge._3) = new Array[ArrayBuffer[(String, Int, Int, Int, Int)]](25000)
      var idIn = ge._4 / 10000
      if (genExonsMap(ge._3)(idIn) == null)
        genExonsMap(ge._3)(idIn) = new ArrayBuffer[(String, Int, Int, Int, Int)]()
      genExonsMap(ge._3)(idIn) += ((ge._1, ge._2, ge._4, ge._5, ge._7))
    }
    return genExonsMap
  }

  /**
   *
   * @param iRegionId
   * @return
   */
  def regionIdToGenExonId(iRegionId: Long): (String, Int) = {

    //remove sampleId header
    val regId = iRegionId % 1000000000000L
    var geneExonId: (String, Int) = ("", 0)
    val newRegPreffix = "NEWREG"
    val knowGenPreffix = "ENSG"
    val nameLength = 15

    val exonId = (regId % 1000).toInt
    val genId = (regId / 100000).toString

    if (exonId == 0) //check if exonId=0 =>new region
      geneExonId = (newRegPreffix.padTo(nameLength - genId.length, '0') + genId, exonId)
    else {
      geneExonId = (knowGenPreffix.padTo(nameLength - genId.length, '0') + genId, exonId)
    }

    return (geneExonId)
  }

  def sampleToLong(iSampleid: Int): Long = {

    return (iSampleid * 1000000000000L)
  }

  def stripSampleID(positionID: Long): Long = {

    return (positionID % 1000000000000L)
  }
}
