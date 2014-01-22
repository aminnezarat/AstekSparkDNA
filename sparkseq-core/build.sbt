name := "sparkseq"

version := "0.1"

scalaVersion := "2.9.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "0.8.1-incubating",
  "org.scalatest" %% "scalatest" % "1.9.1" % "test",
  "com.github.tototoshi" %% "scala-csv" % "1.0.0-SNAPSHOT",
  "org.apache.commons" % "commons-math3" % "3.2",          
  "org.apache.hadoop" % "hadoop-client" % "1.1.2",
  "fi.tkk.ics.hadoop.bam" % "hadoop-bam" % "6.1-SNAPSHOT",
  "picard" % "picard" % "1.93",
  "samtools" % "samtools" % "1.93",
  "tribble" % "tribble" % "1.93",
  "variant" % "variant" % "1.93" 
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

