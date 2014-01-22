package pl.elka.pw.sparkseq.statisticalTests

/**
 * Created by marek on 1/22/14.
 */
object SparkSeqStats {
  def mean(xs: Seq[Int]): Double = xs match {
    case Nil => 0.0
    case ys => ys.reduceLeft(_ + _) / ys.size.toDouble
  }
  def stddev(xs: Seq[Int], avg: Double): Double = xs match {
    case Nil => 0.0
    case ys => math.sqrt((0.0 /: ys) {
      (a,e) => a + math.pow(e - avg, 2.0)
    } / xs.size)
  }

}
