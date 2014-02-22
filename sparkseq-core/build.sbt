import scala.util.Properties

name := "sparkseq-core"

version := "0.1"

scalaVersion := "2.10.3"

val DEFAULT_HADOOP_VERSION = "1.2.1"
lazy val hadoopVersion = Properties.envOrElse("SPARK_HADOOP_VERSION", DEFAULT_HADOOP_VERSION)

ScctPlugin.instrumentSettings

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "0.9.0-incubating",
  "org.scalatest" % "scalatest_2.10" % "2.1.0-RC2" % "test",
  "org.apache.commons" % "commons-math3" % "3.2",          
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
  "fi.tkk.ics.hadoop.bam" % "hadoop-bam" % "6.1-SNAPSHOT",
  "picard" % "picard" % "1.93",
  "samtools" % "samtools" % "1.93",
  "tribble" % "tribble" % "1.93",
  "variant" % "variant" % "1.93",
  "com.github.nscala-time" %% "nscala-time" % "0.8.0"
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "Scala Tools Snapshots" at "http://scala-tools.org/repo-snapshots/",
  "ScalaNLP Maven2" at "http://repo.scalanlp.org/repo",
  "Spray" at "http://repo.spray.cc",
  "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
  "Hadoop-BAM" at "http://hadoop-bam.sourceforge.net/maven/" 
)

testOptions in Test <+= (target in Test) map {
  t => Tests.Argument(TestFrameworks.ScalaTest, "junitxml(directory=\"%s\")" format (t / "test-reports"))
}
