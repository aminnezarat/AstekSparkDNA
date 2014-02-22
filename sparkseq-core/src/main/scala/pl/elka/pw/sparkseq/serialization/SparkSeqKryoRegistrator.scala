/**
 * Copyright (c) 2014. [insert your company or name here]
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

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
//import com.romix.scala.serialization.kryo._
/**
 * Class for registering various classes with KryoSerializer.
 */
class SparkSeqKryoRegistrator extends org.apache.spark.serializer.KryoRegistrator {
  /**
   * Method for registering various classes with KryoSerializer.
   * @param kryo
   */
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[fi.tkk.ics.hadoop.bam.SAMRecordWritable])
    kryo.register(classOf[net.sf.samtools.Cigar])
    kryo.register(classOf[fi.tkk.ics.hadoop.bam.BAMInputFormat])
    kryo.register(classOf[org.apache.hadoop.io.LongWritable])
    //kryo.register(classOf[scala.collection.Traversable[_]], new ScalaCollectionSerializer(kryo))
    //kryo.register(classOf[scala.Product], new ScalaProductSerializer(kryo))
   }
}