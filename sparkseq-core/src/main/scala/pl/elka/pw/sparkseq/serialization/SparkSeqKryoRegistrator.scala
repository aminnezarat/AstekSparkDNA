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