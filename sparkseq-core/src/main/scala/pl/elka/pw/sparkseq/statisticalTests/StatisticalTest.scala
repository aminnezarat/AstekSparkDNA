package pl.elka.pw.sparkseq.statisticalTests

/**
 * Created by mesos on 3/9/14.
 */
abstract class StatisticalTest {

  def getTestStatistics(x: Seq[Int], y: Seq[Int]): Double

  //def getPValue(iTestStat: Double): Double

}
