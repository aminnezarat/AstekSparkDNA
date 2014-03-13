package pl.elka.pw.sparkseq.permutationTests

/**
 * Created by mesos on 3/9/14.
 */
abstract class PermutationTest {

  def getTestStatistics(): Double

  def getPValue(iTestStat: Double): Double

}
