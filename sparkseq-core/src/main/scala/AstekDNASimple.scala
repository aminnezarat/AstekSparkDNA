/**
  * Created by Nezarat on 8/11/2017.
  */
import seq.am.yz.astekdna.seqAnalysis.AstekSeqAnalysis
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import seq.am.yz.astekdna.serialization.AstekSeqKryoProperties
import seq.am.yz.astekdna.util.AstekSeqContexProperties

object AstekDNASimple {
  def main(args: Array[String]) {

    AstekSeqContexProperties.setupContexProperties()
    AstekSeqKryoProperties.setupKryoContextProperties()
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SimpleAstekDNA")
    val sc = new SparkContext("local[2]","My app",sparkConf)
    val seqAnalysis = new AstekSeqAnalysis(sc, "E:\\AstekSparkDNA\\datastore\\orbFrontalF1_Y.bam", 1, 1, 1)
    println(seqAnalysis.getCoverageBase().filter(p => (p._2 >= 10)).count())
  }
}
