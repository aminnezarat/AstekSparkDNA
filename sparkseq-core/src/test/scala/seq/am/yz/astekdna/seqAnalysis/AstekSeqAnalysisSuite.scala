package seq.am.yz.astekdna.seqAnalysis

import seq.am.yz.astekdna.util.AstekFunSuite
import seq.am.yz.astekdna.conversions.AstekSeqConversions
import seq.am.yz.astekdna.util.AstekFunSuite


import org.scalatest.FunSuite
class AstekSeqAnalysisSuite extends  AstekFunSuite{

      sparkSeqTest("Test region count"){
        val pathExonsList = "E:\\AstekSparkDNA\\sparkseq-core\\src\\test\\resources\\sample_1.bed"
        val genExonsMapB = sc.broadcast(AstekSeqConversions.BEDFileToHashMap(sc,pathExonsList ))
        val seqAnalysis = new AstekSeqAnalysis(sc,"E:\\AstekSparkDNA\\sparkseq-core\\src\\test\\resources\\sample_1.bam",25,1,4)
        val cov = seqAnalysis.getCoverageRegion(genExonsMapB)
        assert(seqAnalysis.getCoverageRegion(genExonsMapB).first()._2 === 3)
      }

      sparkSeqTest("Test base count"){
        val seqAnalysis = new AstekSeqAnalysis(sc,"E:\\AstekSparkDNA\\sparkseq-core\\src\\test\\resources\\sample_1.bam",25,1,4)
        assert(seqAnalysis.getCoverageBase.filter(r=>(r._1==25001000011240L) ).first()._2 === 3)
      }

      sparkSeqTest("Test base count with filtered region"){
        val seqAnalysis = new AstekSeqAnalysis(sc,"E:\\AstekSparkDNA\\sparkseq-core\\src\\test\\resources\\sample_1.bam",25,1,4)
        assert(seqAnalysis.getCoverageBaseRegion("chr1",11000,16000).filter(r=>(r._1==25001000011240L) ).first()._2 === 3)

      }
 }
