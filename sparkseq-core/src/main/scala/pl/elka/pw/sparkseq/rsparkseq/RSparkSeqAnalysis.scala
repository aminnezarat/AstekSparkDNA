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
package pl.elka.pw.sparkseq.rsparkseq

import pl.elka.pw.sparkseq.seqAnalysis.SparkSeqAnalysis
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import pl.elka.pw.sparkseq.conversions.SparkSeqConversions
import pl.elka.pw.sparkseq.util.SparkSeqRegType._

/**
 * Created by mwiewiorka on 3/31/14.
 */

/**
 * Class for RSparkSeq R-package
 * @param iSC Apache Spark context.
 * @param iBAMFile  Path to the first BAM file.
 * @param iSampleId  Id of the firs sample (must be numeric).
 * @param iNormFactor  Normalization factor for doing count normalization between samples.
 * @param iReduceWorkers  Number of Reduce workers for doing transformations such as sort or join (see
 *                        http://spark.incubator.apache.org/docs/latest/scala-programming-guide.html for details).
 */
class RSparkSeqAnalysis(iSC: SparkContext, iBAMFile: String, iSampleId: Int, iNormFactor: Double, iReduceWorkers: Int = 8)
  extends SparkSeqAnalysis(iSC, iBAMFile, iSampleId, iNormFactor, iReduceWorkers) {

  /**
   * Get all samples of the RSparkSeqAnalysis object
   * @return Array of paths.
   */
  def getRSamples(): Array[String] = {

    return (super.getSamples().collect())
  }

  /**
   * Get base coverage for the given region
   * @param chr Chromosome (eg. chr1)
   * @param regStart Starting position in a chromosome.
   * @param regEnd End position in a chromosome.
   * @return RDD of tuples (genID, coverage)
   */
  def getRBaseCoverageRegion(chr: String, regStart: Int, regEnd: Int): Array[(Long, Int)] = {

    return (super.getCoverageBaseRegion(chr, regStart, regEnd).collect())
  }

  /**
   * Get base coverage for all positions
   * @return RDD of tuples (genID, coverage)
   */
  def getRBaseCoverage(): Array[(Long, Int)] = {

    return (super.getCoverageBase().collect())
  }

  /**
   * Get feature counts for a given list of regions of interest
   * @param iGenExons A Spark broadcast variable created from BED file that is transformed using SparkSeqConversions.BEDFileToHashMap
   * @param unionMode If set to true reads overlapping more than one region are discarded (false by default). More info on union mode:
   *                  http://www-huber.embl.de/users/anders/HTSeq/doc/count.html#count
   * @return RDD of tuples (regionId, coverage)
   */
  def getRRegionCoverage(iGenExons: org.apache.spark.broadcast.Broadcast[scala.collection.mutable.
  HashMap[String, Array[scala.collection.mutable.ArrayBuffer[(String, String, Int, Int)]]]], unionMode: Boolean = false): Array[(Long, Int)] = {

    return (super.getCoverageRegion(iGenExons).collect())
  }

  /**
   * Get gene feature counts
   * @return array of tuples(geneID,array of counts sorted by sampleID)
   */
  def getRGeneCoverage(): Array[(String, Array[Int])] = {

    return getGeneCoverage(super.getRegionMap())
  }

  /**
   * Get gene feature counts for genes specified in an input array
   * @param geneArray array of string genes identifiers
   * @return
   */
  def getRGeneCoverage(geneArray: Array[String]): Array[(String, Array[Int])] = {

    val regionCovRDD = getCoverageRegion(super.getRegionMap())
    regionCovRDD.cache()
    val regionCovFilterRDD = regionCovRDD
      .filter(r => geneArray.contains(SparkSeqConversions.ensemblRegionIdToExonId(r._1, Gene)))
    return super.getCoverageTable(regionCovFilterRDD, Gene)
  }

  /**
   * Get exon feature counts
   * @return array of tuples(exonID,array of counts sorted by sampleID)
   */
  def getRExonCoverage(): Array[(String, Array[Int])] = {

    return getExonCoverage(super.getRegionMap())
  }
}
