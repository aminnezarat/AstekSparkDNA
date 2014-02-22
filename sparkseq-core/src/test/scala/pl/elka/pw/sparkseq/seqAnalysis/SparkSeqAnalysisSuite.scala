package pl.elka.pw.sparkseq.seqAnalysis

import  pl.elka.pw.sparkseq.util.SparkFunSuite
import pl.elka.pw.sparkseq.conversions.SparkSeqConversions

/**
 * Created by marek on 1/22/14.
 */

import org.scalatest.FunSuite
    class SparkSeqAnalysisSuite extends  SparkFunSuite{

      sparkSeqTest("Test region count"){
        val pathExonsList = "sparkseq-core/src/test/resources/sample_1.bed"
        val genExonsMapB = sc.broadcast(SparkSeqConversions.BEDFileToHashMap(sc,pathExonsList ))
        val seqAnalysis = new SparkSeqAnalysis(sc,"sparkseq-core/src/test/resources/sample_1.bam",25,1,4)
        val cov = seqAnalysis.getCoverageRegion(genExonsMapB)
        assert(seqAnalysis.getCoverageRegion(genExonsMapB).first()._2 === 3)
      }

      sparkSeqTest("Test base count"){
        val seqAnalysis = new SparkSeqAnalysis(sc,"sparkseq-core/src/test/resources/sample_1.bam",25,1,4)
        assert(seqAnalysis.getCoverageBase.filter(r=>(r._1==25001000011240L) ).first()._2 === 3)
      }

      sparkSeqTest("Test base count with filtered region"){
        val seqAnalysis = new SparkSeqAnalysis(sc,"sparkseq-core/src/test/resources/sample_1.bam",25,1,4)
        assert(seqAnalysis.getCoverageBaseRegion("chr1",11000,16000).filter(r=>(r._1==25001000011240L) ).first()._2 === 3)

      }
 }
