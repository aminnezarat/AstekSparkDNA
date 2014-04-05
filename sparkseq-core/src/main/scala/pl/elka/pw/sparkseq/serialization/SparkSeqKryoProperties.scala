/**
 * Copyright (c) 2014. Marek Wiewiorka
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pl.elka.pw.sparkseq.serialization

/**
 * Object for setting various KryoSerializer properties.
 */
object SparkSeqKryoProperties {

  /**
   * Method used before creating Apache Spark context for setting various KryoSerializer properties.
   * @return
   */
  def setupKryoContextProperties() = {
    System.setProperty("spark.kryoserializer.buffer.mb", "100")
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "pl.elka.pw.sparkseq.serialization.SparkSeqKryoRegistrator")
    System.setProperty("spark.kryo.referenceTracking","false")
  }

}
