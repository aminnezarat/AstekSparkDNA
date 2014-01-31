package pl.elka.pw.sparkseq.statisticalTests

/**
 * Object for computing basic sample statistics.
 */
object SparkSeqStats {
  /**
   * Method for computing mean value of a sample.
   * @param xs Sample sequence
   * @return Mean value of a sample.
   */
  def mean(xs: Seq[Int]): Double = xs match {
    case Nil => 0.0
    case ys => ys.reduceLeft(_ + _) / ys.size.toDouble
  }

  /**
   * Method for computing standard deviation of a sample.
   * @param xs Sample sequence
   * @param avg Mean value of a sample.
   * @return Standard deviation of a sample.
   */
  def stddev(xs: Seq[Int], avg: Double): Double = xs match {
    case Nil => 0.0
    case ys => math.sqrt((0.0 /: ys) {
      (a,e) => a + math.pow(e - avg, 2.0)
    } / xs.size)
  }

}
