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
package pl.elka.pw.sparkseq.rparkseq

import pl.elka.pw.sparkseq.seqAnalysis.SparkSeqAnalysis
import org.apache.spark.SparkContext

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


}
